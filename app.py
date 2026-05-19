"""
Kubernetes-deployed ML inference service.

Implements a FastAPI inference API backed by Ray async actors. The
service demonstrates production-style serving patterns including
concurrent request handling, micro-batching, explicit backpressure,
health checks, and runtime metrics.

The inference computation is intentionally lightweight so the project
focuses on serving infrastructure rather than model accuracy.
"""

import asyncio
import os

import ray
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from ray.exceptions import RayTaskError


# ------------------------------------------------------------------
# Configuration.
# ------------------------------------------------------------------

def _env_int(name: str, default: int) -> int:
    """
    Read an integer configuration value from the environment.
    """
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    """
    Read a float configuration value from the environment.
    """
    return float(os.getenv(name, str(default)))


# Tunable serving parameters.
MAX_QUEUE = _env_int("MAX_QUEUE", 1000)
MAX_INFLIGHT = _env_int("MAX_INFLIGHT", 200)
REQUEST_TIMEOUT_S = _env_float("REQUEST_TIMEOUT_S", 5.0)
ACQUIRE_TIMEOUT_S = _env_float("ACQUIRE_TIMEOUT_S", 0.001)
NUM_ACTORS = _env_int("NUM_ACTORS", 4)

# Service lifecycle flag used by readiness and request handling.
shutting_down = False


class OverloadError(Exception):
    """
    Raised when an actor queue reaches its configured capacity.
    """
    pass


# ------------------------------------------------------------------
# Ray actor: async batch processor.
# ------------------------------------------------------------------

@ray.remote
class AsyncBatchProcessor:
    """
    Stateful Ray actor that queues, batches, and processes requests.

    Each actor maintains its own bounded queue. Requests are stored with
    futures so each caller can await its individual result after batch
    processing completes.
    """

    def __init__(self, max_batch_size=4, batch_wait_s=0.05):
        self.max_batch_size = max_batch_size
        self.batch_wait_s = batch_wait_s

        self.queue: list[tuple[int, asyncio.Future]] = []
        self.lock = asyncio.Lock()
        self.processing_task: asyncio.Task | None = None

        self.total_processed = 0
        self.total_enqueued = 0
        self.total_rejected = 0

    async def process(self, item: int) -> int:
        """
        Enqueue one request and wait for its batched result.

        If the actor queue is full, reject the request instead of
        allowing unbounded backlog growth.
        """
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        async with self.lock:
            if len(self.queue) >= MAX_QUEUE:
                self.total_rejected += 1
                raise OverloadError(f"Queue full (>{MAX_QUEUE}). Please retry.")

            self.queue.append((item, fut))
            self.total_enqueued += 1

            if self.processing_task is None:
                self.processing_task = asyncio.create_task(self._run())

        return await fut

    async def _run(self) -> None:
        """
        Collect and process a micro-batch of queued requests.
        """
        await asyncio.sleep(self.batch_wait_s)

        async with self.lock:
            batch = self.queue[:self.max_batch_size]
            self.queue = self.queue[self.max_batch_size:]

            if self.queue:
                self.processing_task = asyncio.create_task(self._run())
            else:
                self.processing_task = None

        items = [item for item, _ in batch]
        results = self._process_batch(items)

        for (_, fut), result in zip(batch, results):
            fut.set_result(result)

    def _process_batch(self, items: list[int]) -> list[int]:
        """
        Process one batch.

        This placeholder computation represents the model inference step.
        """
        self.total_processed += len(items)
        print(f"Processing batch {items}, total={self.total_processed}")

        return [item * 2 for item in items]

    async def stats(self) -> dict:
        """
        Return actor-level queue and throughput statistics.
        """
        async with self.lock:
            queue_len = len(self.queue)
            processing_scheduled = self.processing_task is not None

        return {
            "queue_len": queue_len,
            "max_queue": MAX_QUEUE,
            "max_batch_size": self.max_batch_size,
            "batch_wait_s": self.batch_wait_s,
            "processing_scheduled": processing_scheduled,
            "total_enqueued": self.total_enqueued,
            "total_rejected": self.total_rejected,
            "total_processed": self.total_processed,
        }


# ------------------------------------------------------------------
# FastAPI application.
# ------------------------------------------------------------------

app = FastAPI()

processors: list = []
rr_index = 0
rr_lock: asyncio.Lock | None = None

inflight_sem: asyncio.Semaphore | None = None


class InferRequest(BaseModel):
    """
    Request schema for /infer.
    """
    value: int


class InferResponse(BaseModel):
    """
    Response schema for /infer.
    """
    result: int


def _service_ready() -> bool:
    """
    Return whether the service has the minimum state needed to receive traffic.
    """
    return bool(processors) and inflight_sem is not None and rr_lock is not None


async def pick_actor():
    """
    Select a Ray actor using round-robin routing.
    """
    global rr_index

    if rr_lock is None or not processors:
        raise RuntimeError("Service not ready")

    async with rr_lock:
        actor = processors[rr_index % len(processors)]
        rr_index += 1

    return actor


# ------------------------------------------------------------------
# Lifecycle hooks.
# ------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    """
    Initialize Ray, actor pool, and service-level concurrency control.
    """
    global inflight_sem, rr_lock, processors

    ray.init(ignore_reinit_error=True)

    processors = [AsyncBatchProcessor.remote() for _ in range(NUM_ACTORS)]
    inflight_sem = asyncio.Semaphore(MAX_INFLIGHT)
    rr_lock = asyncio.Lock()

    print(f"Ray initialized and {NUM_ACTORS} processors started")


@app.on_event("shutdown")
async def shutdown():
    """
    Stop accepting new work and shut down Ray.
    """
    global shutting_down
    shutting_down = True

    await asyncio.sleep(1.0)
    ray.shutdown()


# ------------------------------------------------------------------
# Health endpoints.
# ------------------------------------------------------------------

@app.get("/healthz/live")
async def live():
    """
    Liveness probe endpoint.

    Indicates that the process is running.
    """
    return {"ok": True}


@app.get("/healthz/ready")
async def ready():
    """
    Readiness probe endpoint.

    Indicates whether the service is safe to receive traffic.
    """
    if shutting_down:
        raise HTTPException(status_code=503, detail="Shutting down")

    if not _service_ready():
        raise HTTPException(status_code=503, detail="Not ready")

    if not ray.is_initialized():
        raise HTTPException(status_code=503, detail="Ray not initialized")

    try:
        await asyncio.wait_for(processors[0].stats.remote(), timeout=0.5)
    except Exception:
        raise HTTPException(status_code=503, detail="Actor not responding")

    return {"ok": True}


# ------------------------------------------------------------------
# Inference endpoint.
# ------------------------------------------------------------------

@app.post("/infer", response_model=InferResponse)
async def infer(req: InferRequest):
    """
    Run one inference request through the actor pool.

    The endpoint applies service-level backpressure before routing work
    to an actor. Actor-level queue limits provide a second layer of
    overload protection.
    """
    if shutting_down:
        raise HTTPException(status_code=503, detail="Shutting down")

    if not _service_ready():
        raise HTTPException(status_code=503, detail="Service not ready")

    try:
        await asyncio.wait_for(inflight_sem.acquire(), timeout=ACQUIRE_TIMEOUT_S)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=429, detail="Busy (inflight limit). Please retry.")

    try:
        actor = await pick_actor()

        result = await asyncio.wait_for(
            actor.process.remote(req.value),
            timeout=REQUEST_TIMEOUT_S,
        )

        return InferResponse(result=result)

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Request timed out")

    except RayTaskError as e:
        try:
            e.as_instanceof_cause(OverloadError)
        except Exception:
            raise HTTPException(status_code=500, detail=str(e))
        raise HTTPException(status_code=429, detail="Busy (actor queue full). Please retry.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        inflight_sem.release()


# ------------------------------------------------------------------
# Metrics endpoint.
# ------------------------------------------------------------------

@app.get("/metrics")
async def metrics():
    """
    Return service-level and actor-level runtime metrics.
    """
    if not _service_ready():
        raise HTTPException(status_code=503, detail="Service not ready")

    actor_stats = await asyncio.gather(*(p.stats.remote() for p in processors))

    inflight_available = getattr(inflight_sem, "_value", None)

    return {
        "service": {
            "num_actors": len(processors),
            "max_inflight": MAX_INFLIGHT,
            "inflight_available": inflight_available,
            "inflight_in_use_est": (
                None
                if inflight_available is None
                else (MAX_INFLIGHT - inflight_available)
            ),
            "request_timeout_s": REQUEST_TIMEOUT_S,
        },
        "actor": actor_stats,
    }
