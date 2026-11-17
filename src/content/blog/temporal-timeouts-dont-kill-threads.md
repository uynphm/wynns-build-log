---
title: "Temporal timeouts don't kill threads"
description: 'How one thread and an unset default made Temporal activities time out without ever running, a 100-line script that reproduces it, and how I set up workers and task queues now.'
pubDate: 'Nov 17 2026'
---

I had a Temporal worker running two kinds of activities. Some called a slow external service and could take minutes; others were quick bookkeeping, like updating a status, and finished in well under a second. They were plain sync Python, so the worker ran them on a thread pool, and that thread pool had one thread. I never thought about that number much. One thread meant one thing at a time, things would queue, and Temporal would retry whatever needed retrying. That was the whole mental model, and it held up fine until the day the external service got slow.

I expected the activities that called it to be slow too, and they were. What I didn't expect was the quick ones failing. A status update that normally takes milliseconds timed out three times in a row, and then on attempt 4 it finished the second it started. Around the same time, some of the slow work finished after Temporal had already marked its workflow as failed. Neither of those should be possible. A call that takes milliseconds can't time out three times, and a workflow that has failed shouldn't still be doing work.

The attempt history turned out to be the whole answer, once I read it the right way. An activity that finishes instantly on attempt 4, after three timeouts, was never slow. **It never ran.** It was waiting in line somewhere with its timeout already ticking, and every stuck activity had been waiting on the same worker. So I opened that worker's config. One thread, as planned. And `max_concurrent_activities` wasn't set, which I hadn't thought twice about, and which was the bug.

## Two numbers, not one

A Temporal worker decides how much work to take on with two separate settings, and I had only been thinking about one of them. The thread pool you pass as `activity_executor` decides how many sync activities can run at the same time. `max_concurrent_activities` decides how many activity tasks the worker will accept from Temporal at the same time. If you don't set it, the Python SDK sets it for you, and **the default is 100**. The comment in `temporalio/worker/_tuning.py` says it plainly: "Any unspecified slot numbers will default to 100."

So my worker looked like this:

```python
worker = Worker(
    client,
    task_queue="activities",
    activities=[call_slow_service, update_status],
    activity_executor=ThreadPoolExecutor(max_workers=1),
    # max_concurrent_activities not set, so 100
)
```

It would accept up to 100 activities and run one. The other 99 sat in the thread pool's own queue, on the worker, out of Temporal's sight. That would be harmless if the timeout clock waited for a thread, but it doesn't: `start_to_close_timeout` **starts when the worker accepts the task**. Every status update accepted behind a stuck slow call spent its whole timeout in that queue and failed without running a line of code.

## Seeing it for myself

I wanted to watch it happen rather than take my own word for it, so I wrote a small script against a local Temporal dev server. It has two activities. `slow_call` sleeps for 20 seconds. `quick_update` does nothing at all, except print which attempt it is and how long ago the worker accepted it. It gets a 5-second `start_to_close_timeout` and retries every second. The worker has one thread. I start the slow one, wait a second, start the quick one, and run the whole thing three times with a different `max_concurrent_activities` each time.

Here is the run with it left unset:

```text
=== 1 thread, max_concurrent_activities = unset (default 100) ===
SDK warning: none
[  0.0s] slow_call    starts running (takes 20s)
[  1.0s] quick_update scheduled (timeout 5s, normally takes ~0s)
[ 20.0s] slow_call    done
[ 20.0s] quick_update attempt 1 finally runs, 19.0s after the worker accepted it
[ 20.0s] quick_update attempt 2 finally runs, 13.0s after the worker accepted it
[ 20.0s] quick_update attempt 3 finally runs, 7.0s after the worker accepted it
[ 20.0s] quick_update attempt 4 finally runs, 1.0s after the worker accepted it
[ 20.0s] QuickWorkflow completed
```

That is my bad day in ten lines. Attempt 1 was accepted at 1 second and timed out at 6, while still waiting for the thread. Attempts 2 and 3 were accepted 6 seconds apart, each one second after the previous one timed out, and each one timed out in the queue too. Attempt 4 got lucky with the timing. And then, at 20 seconds, when the thread finally freed up, **all four attempts ran**, including the three that Temporal had already given up on. The worker even logged it: `Activity not found on completion. This may happen if the activity has already been cancelled but completed anyway.`

Here is the same script with `max_concurrent_activities=1`:

```text
=== 1 thread, max_concurrent_activities = 1 ===
SDK warning: none
[  0.0s] slow_call    starts running (takes 20s)
[  1.0s] quick_update scheduled (timeout 5s, normally takes ~0s)
[ 20.0s] slow_call    done
[ 20.0s] quick_update attempt 1 finally runs, 0.0s after the worker accepted it
[ 20.0s] QuickWorkflow completed
```

Same 19-second wait, but this time it happened on the Temporal server, where no timeout was running. The worker accepted the task only when it had a thread for it, and attempt 1 ran in 0.0 seconds. One run, no retries, no leftovers.

The third run set `max_concurrent_activities=2`, one more than the thread count. That is the only case where the SDK said anything: `Worker max_concurrent_activities is 2 but activity_executor's max_workers is only 1`. Leave it unset, and the gap is 100 to 1 with no warning at all.

## Why the timeout didn't save me

The run above also explains the second strange thing from that day, the slow work that finished after its workflow had failed. I had assumed a timeout stops the code. It doesn't. When `start_to_close_timeout` fires, Temporal marks that attempt as failed and schedules a retry, and that is all it does. An attempt still waiting in the thread pool's queue has no thread yet, so there is nothing to interrupt, and when a thread frees up it runs anyway, exactly like attempts 1 to 3 above. An attempt that is already running can get a `CancelledError` raised inside its thread, but Python only delivers that when the thread runs Python code again, and a thread blocked on a socket read or a lock isn't. Either way, the old attempt carries on after Temporal has moved on. Its result is thrown away, but whatever it changed on the way, it changed.

## How I set up workers now

The first change is the one the demo makes obvious: **set `max_concurrent_activities` to the size of the thread pool**, every time, on every worker that runs sync activities. Then the worker only accepts what it can run, and everything else waits on the server, where its timeout hasn't started yet. It is one line, and it would have saved me the whole day.

```python
THREADS = 8

worker = Worker(
    client,
    task_queue="activities",
    activities=[call_slow_service, update_status],
    activity_executor=ThreadPoolExecutor(max_workers=THREADS),
    max_concurrent_activities=THREADS,
)
```

The second is to stop relying on Temporal's timeout to end a blocking call, since it can't. Every HTTP request gets its own timeout, every lock gets an acquire timeout, and both are shorter than the activity's `start_to_close_timeout`, so by the time Temporal gives up, the thread is already on its way out. And because attempts can outlive their timeout, I write every activity as if it might run twice. In the demo, `quick_update` ran four times for one successful call.

## How I set up task queues now

Even with the numbers matched, one thing in my setup was still wrong: a status update that takes milliseconds shared a queue, and a worker, with calls that take minutes. In the second demo run, `quick_update` still waited 19 seconds; it just waited somewhere safer. However many threads a worker has, a burst of slow calls can fill all of them, and the quick work waits behind it. So now work that takes different amounts of time goes on different task queues, each with its own worker sized for it, and the workflow picks the queue on each call:

```python
result = await workflow.execute_activity(
    call_slow_service, item_id,
    task_queue="slow",
    start_to_close_timeout=timedelta(minutes=10),
)
await workflow.execute_activity(
    update_status, item_id,
    task_queue="fast",
    start_to_close_timeout=timedelta(seconds=30),
)
```

A slow dependency can now only slow down its own queue. The one trap left is a bottleneck behind the threads: if every thread shares something that only handles one request at a time, like a single shared client or connection, adding threads just moves the line somewhere else. Each thread needs its own.

## The tell

If I had to keep one thing from all of this, it is how to read the attempt history. When an activity times out a few times and then finishes in a second, **it was never slow. It was waiting for a thread.** Before looking at the code, the network or the service it calls, I now check the worker's `max_concurrent_activities` against its thread pool. It takes ten seconds, and it is the first thing I would have wanted to know that day.
