# /// script
# requires-python = ">=3.12"
# dependencies = ["temporalio==1.27.2"]
# ///
"""One thread, 100 slots: watch a quick activity time out without ever running.

Needs a local Temporal dev server:  temporal server start-dev --port 7311
Run:                                uv run --script demo.py
"""

import asyncio
import sys
import time
import uuid
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.worker import Worker

T0 = time.monotonic()


def now() -> str:
    return f"{time.monotonic() - T0:5.1f}s"


@activity.defn
def slow_call() -> None:
    print(f"[{now()}] slow_call    starts running (takes 20s)")
    time.sleep(20)
    print(f"[{now()}] slow_call    done")


@activity.defn
def quick_update() -> None:
    info = activity.info()
    waited = (time.time() - info.started_time.timestamp())
    print(
        f"[{now()}] quick_update attempt {info.attempt} finally runs, "
        f"{waited:.1f}s after the worker accepted it"
    )


@workflow.defn
class SlowWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(slow_call, start_to_close_timeout=timedelta(seconds=60))


@workflow.defn
class QuickWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            quick_update,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=1.0, maximum_attempts=10),
        )


async def run_case(client: Client, slots: int | None) -> None:
    global T0
    queue = f"demo-{uuid.uuid4().hex[:6]}"
    label = "unset (default 100)" if slots is None else str(slots)
    print(f"\n=== 1 thread, max_concurrent_activities = {label} ===")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[SlowWorkflow, QuickWorkflow],
            activities=[slow_call, quick_update],
            activity_executor=ThreadPoolExecutor(max_workers=1),
            max_concurrent_activities=slots,
        )
    sdk_warnings = [str(w.message) for w in caught if "max_concurrent_activities" in str(w.message)]
    print(f"SDK warning: {sdk_warnings[0] if sdk_warnings else 'none'}")

    async with worker:
        T0 = time.monotonic()
        slow = await client.start_workflow(SlowWorkflow.run, id=f"slow-{queue}", task_queue=queue)
        await asyncio.sleep(1)
        print(f"[{now()}] quick_update scheduled (timeout 5s, normally takes ~0s)")
        quick = await client.start_workflow(QuickWorkflow.run, id=f"quick-{queue}", task_queue=queue)
        try:
            await quick.result()
            print(f"[{now()}] QuickWorkflow completed")
        except Exception as e:
            print(f"[{now()}] QuickWorkflow failed: {e}")
        await slow.result()
        # Let any leftover attempts drain so they show up in the log.
        await asyncio.sleep(3)


async def main() -> None:
    client = await Client.connect("localhost:7311")
    for slots in (None, 1, 2):
        await run_case(client, slots)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
