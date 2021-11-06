module fluffy.app;

import core.memory : GC;
import fluffy.taskfiber;
import fluffy.ticket;
import fluffy.taskqueue;
import core.thread;
import core.atomic;
import core.stdc.stdio;
import core.sys.posix.signal : timespec;
import core.sys.posix.time;
import core.time;
import fluffy.tracy;

static  immutable task_function_t terminationDg =
    (Task*) { };

uint enqueueTermination(shared (TaskQueue)* q, string terminationMessage)
{
    // little guard to we don't push the message if the chance of success is low
    if (q.tasksInQueue() > (q.queue.length - 4))
        return false;

    auto terminationTaskP = cast(Task*)alloc.alloc(Task.sizeof);
    (*terminationTaskP) = Task(terminationDg, cast(shared void*) pushString(terminationMessage));

    return q.push(&terminationTaskP);
}
void micro_sleep(uint micros)
{
    timespec ts;
    ts.tv_nsec = micros * 1000;
    nanosleep(&ts, null);
}


struct Alloc
{
    uint initialSize;
    ubyte* memPtr;
    uint capacity_remaining;
    shared TicketCounter allocLock;

    this(uint size)
    {
        import core.stdc.stdlib;
        size = cast(uint) align16(size);
        memPtr = cast(ubyte*) malloc(size);
        initialSize = size;

        capacity_remaining = size;
    }

    ubyte* alloc(uint sz, int line = __LINE__, string file = __FILE__) shared
    {
        auto ticket = allocLock.drawTicket();
        while (!allocLock.servingMe(ticket)) {}

        atomicFence!(MemoryOrder.seq)();
        scope(exit) allocLock.releaseTicket(ticket);

        sz = cast(uint)align16(sz);
        assert(capacity_remaining > sz, "not enough memory allocated for alloc in " ~ file);
        ubyte* result = cast(ubyte*)(memPtr);
        (cast()memPtr) += sz;
        (cast()capacity_remaining) -= sz;

        atomicFence!(MemoryOrder.seq)();
        return result;
    }
}

shared Alloc alloc;

string* pushString(string s)
{
    const sz = cast(uint) (s.length + s.sizeof + 1);

    auto mem = alloc.alloc(sz);
    string* result = cast(string*) mem;
    auto data = cast(char*) (mem + s.sizeof);
    data[0 .. s.length] = s.ptr[0 .. s.length];
    data[s.length] = '\0';
    (*result) = cast(string) data[0 .. s.length];
    return result;
}

uint addTask(Task* task, uint myQueue = uint.max)
{
    mixin(zoneMixin("addTask"));

    /*tls*/ static currentQueue = 0;
    uint pushIntoQueue = currentQueue;

    static immutable queueCutOff = cast(int) (TaskQueue.init.queue.length * (5f/6f));
    if (myQueue != uint.max && g_queues[myQueue].tasksInQueue() < queueCutOff)
    {
        pushIntoQueue = myQueue;
    }

    version (multi_try)
    {
        auto maxAttempts = g_queues.length;

        bool succeses = false;
        while(maxAttempts-- && !succeses)
        {
            succeses = g_queues[currentQueue].push(task);
            if (++currentQueue >= g_queues.length)
                currentQueue = 0;
        }

        return succeses;
    }
    else
    {

        if (++currentQueue >= g_queues.length)
        {
            currentQueue = 0;
        }

        return g_queues[pushIntoQueue].push(&task);
    }
}

extern (C) void breakpoint () {}


struct Worker
{
    Thread workerThread;
    shared bool terminate;
    FiberPool workerFiberPool;
}

extern (C) void ___tracy_set_thread_name( const char* name );

enum threadproc;

shared bool killTheWatcher = false;
shared bool killTheWorkers = false;
shared uint expected_completions = uint.max;
@threadproc void watcherFunction ()
{
    mixin(zoneMixin("watcherTime"));
    // who watches the watchman
    ___tracy_set_thread_name(`Watcher`);

    ulong lastCompletedTasks;
    uint no_progress;

    char[32]* queueStringMem = cast(char[32]*)alloc.alloc(32 * cast(uint)g_workers.length);
    char[32][] worker_queue_strings = queueStringMem[0 .. g_workers.length];

    worker_queue_strings.length = g_workers.length;
    foreach(i, ref workerer_queue_string;worker_queue_strings)
    {
        sprintf(workerer_queue_string.ptr, "queue + active tasks %d\0", cast(int) i);
    }

    TracyMessage("Watcher says Hello!");
    while(!atomicLoad(killTheWatcher))
    {
        lastCompletedTasks = atomicLoad!(MemoryOrder.raw)(completedTasks);
        ___tracy_emit_plot("completedTasks", lastCompletedTasks);
        foreach(i; 0 .. g_workers.length)
        {
            TaskQueue* q = cast(TaskQueue*)g_queues[i];
            ___tracy_emit_plot(worker_queue_strings[i].ptr, g_queues[i].tasksInQueue() + (cast()g_workers[i].workerFiberPool).n_used());
        }

        micro_sleep(1);
        if (lastCompletedTasks >= atomicLoad!(MemoryOrder.raw)(expected_completions))
            break;
    }
    // giving tasks 5 microseconds to take care of unfinished buissness
    micro_sleep(5);

    {
        printf("lastCompletedTasks -- %llu\n",
            lastCompletedTasks);
        // printf("watcher: enquing termination\n");
        mixin(zoneMixin("watcher: enqueueingTermnination"));
        foreach(i; 0 .. g_workers.length)
        {
            // printf("termination for worker %d ... ", cast(int)i);
            bool enqueuedTermination = false;
            while(!enqueuedTermination)
            {
                enqueuedTermination = !!g_queues[i].enqueueTermination("Watcher termination");
            }
            // printf("Termination scheduled\n");
        }
    }
    TracyMessage("Watcher says bye!");
}

private shared TicketCounter globalLock;
private shared uint workersReady = 0;

@threadproc void workerFunction () {
    mixin(zoneMixin("workerFunction"));

    static shared int workerCounter;
    short workerIndex = cast(short)(atomicOp!"+="(workerCounter, 1) - 1);
    char[16] worker_name;
    sprintf(&worker_name[0], "Worker %d", workerIndex);
    printf("%s is starting\n", &worker_name[0]);

    ___tracy_set_thread_name(&worker_name[0]);
    // printf("Startnig: %d\n", workerIndex);
    shared (bool) *terminate = &g_workers[workerIndex].terminate;
    shared(TaskQueue*)* myQueueP = &g_queues[workerIndex];
    TaskQueue.initQueue(myQueueP, cast(short)(workerIndex + 1));
    shared (TaskQueue)* myQueue = *myQueueP;
     FiberPool* fiberPool = cast(FiberPool*)&g_workers[workerIndex].workerFiberPool;
    int[fiberPool.fibers.length] fiberExecCount;
    char[32][fiberPool.fibers.length] fiberNames;
    foreach(fIdx; 0 .. fiberPool.fibers.length)
    {
        sprintf(fiberNames[fIdx].ptr, "%s -- Fiber %d", worker_name.ptr, cast(int)fIdx);
        import fluffy.tracy;
    }

    {
        auto initTicket = globalLock.drawTicket();

        {
            while (!globalLock.servingMe(initTicket)) {}
            atomicFence();
        }
        /*tls*/ fiberPool.initFiberPool();
        atomicOp!"+="(workersReady, 1);
        {
            atomicFence();
            globalLock.releaseTicket(initTicket);
        }
    }

    wait_until_workers_are_ready();

    /*tls*/ int myCounter = 0;
    /*tls*/ Task *task;

    /*tls*/ static uint nextExecIdx;
    while(true)
    {
        // mixin(zoneMixin("WorkerLoop"));
        TaskFiber execFiber;
        if (auto idx = fiberPool.nextFree())
        {
            if (myQueue.pull(&task))
            {
                if (task.fn is terminationDg || killTheWorkers)
                {
                    auto terminationMessage = cast(string*) task.taskData;
                    ___tracy_emit_message("recieved termination signal", "recieved termination signal".length, 0);
                    TracyMessage(*terminationMessage);
                    foreach(fIdx; 0 .. fiberPool.fibers.length)
                    {
                        const eCount = fiberExecCount[fIdx];
                        if (eCount) printf("fiber %d -- exeCount: %d\n", cast(int) fIdx, eCount);
                    }
                    break;
                }
                execFiber = fiberPool.getNextFree();
                execFiber.assignTask(task);
            }
            else if (!fiberPool.n_used)
            {
                // no fibers used
                mixin(zoneMixin("sleepnig"));
                TaskQueue* q = cast(TaskQueue*)myQueue;
                int longest_queue_idx = -1;
                enum work_stealing = false;
                static if (work_stealing)
                {
                    uint max_queue_length = 50; // a vicitm needs to have at least 100 tasks to be considered a target
                    shared(TaskQueue)* victim;
                    foreach(qIdx; 0 .. g_queues.length)
                    {
                        auto canidate = g_queues[qIdx];
                        auto canidate_n_tasks = canidate.tasksInQueue();
                        if (canidate_n_tasks > max_queue_length)
                        {
                            victim = canidate;
                            max_queue_length = canidate_n_tasks;
                        }
                    }
                    if (victim)
                    {
                        auto steal_amount = cast(int)(max_queue_length * (1f/3f));
                        // lock the victim queue;
                        const ticket = victim.queueLock.drawTicket();
                        while(!victim.queueLock.servingMe(ticket)) {}

                        {
                            mixin(zoneMixin("Stealing work"));
                            atomicFence!(MemoryOrder.seq)();

                            auto n_stolen = victim.steal(steal_amount, myQueue, ticket);
                            atomicFence!(MemoryOrder.seq)();
                            victim.queueLock.releaseTicket(ticket);
                        }
                        continue;
                    }
                } // work_stealing
                if (longest_queue_idx == -1)
                {
                    mixin(zoneMixin("Out of work ... no victim ... sleeping"));
                    // there's no-one to steal from
                    // let's sleep and continue later
                    micro_sleep(2);
                    continue;
                }

            }
        }

        if (!execFiber)
        {
            mixin(zoneMixin(`FindNextFiber`));
            // if we didn't add a task just now chose a random fiber to exec
            const nonFree = (~fiberPool.freeBitfield);
            ulong nextExecMask;
            auto localNextIdx = nextExecIdx & (fiberPool.fibers.length - 1);
            // make sure the fiber we chose is used
            for(;;)
            {
                nextExecMask = 1UL << localNextIdx;
                if (nextExecMask & nonFree)
                {
                    execFiber = fiberPool.fibers[localNextIdx];
                    nextExecIdx++;
                    break;
                }
                localNextIdx = (++localNextIdx & (fiberPool.fibers.length - 1));
            }
            assert(execFiber.hasTask, "Chosing fiber which doesn't have a task");
        }
        //___tracy_emit_plot(worker_name.ptr, fiberPool.freeBitfield);
        // execute a fiber in the pool
        {
            fiberExecCount[execFiber.idx]++;
            mixin(zoneMixin("FiberExecution"));
            //printf("executing fiber: %p -- idx:%d\n", execFiber, execFiber.idx);
            //printf("stateBeforeExec: %s\n", execFiber.stateToString(execFiber.state()).ptr);
            assert(execFiber.state() == execFiber.state().HOLD, execFiber.stateToString(execFiber.state()));
            version (MARS)
            {
                import dmd.globals;
                // set task_local state to thread_local state
                task_local = execFiber.currentTask.task_local_state;
            }
            execFiber.call();
            version (MARS)
            {
                import dmd.globals;
                // set task_local state to thread_local state
                execFiber.currentTask.task_local_state = task_local;
            }
            // if this completed the fiber we need to to reset it and send it back to the pool
            if (execFiber.state() == execFiber.state().TERM)
            {
                version(MARS)
                {
                    // when we have completed a task we need to put our gaggedErrors into the parent
                    if (auto p = execFiber.currentTask.parent)
                    {
                        p.task_local_state.gaggedErrors += execFiber.currentTask.task_local_state.gaggedErrors;
                    }
                }
                atomicOp!"+="(completedTasks, 1);
                execFiber.hasTask = false;
                fiberPool.free(execFiber);
                execFiber.reset();

            }
        }
    }
    TracyMessage("Goobye!");
}
shared ulong completedTasks;
shared uint n_workers = 0;
shared TaskQueue*[] g_queues;
shared Worker[] g_workers;

void wait_until_workers_are_ready()
{
    assert(atomicLoad(n_workers) != 0);
    for(;;)
    {
        if (atomicLoad!(MemoryOrder.raw)(workersReady) == n_workers)
            break;
        micro_sleep(1);
    }
}

struct WorkersQueuesAndWatcher
{
    shared bool* killTheWatcher;
    shared bool* killTheWorkers;
    shared Thread watcher;
    shared Worker[] workers;
    shared TaskQueue*[] queues;

}

shared TaskQueue* g_queue;

WorkersQueuesAndWatcher fluffy_get_queues(uint n_workers_)
{
    mixin(zoneMixin("Main"));

    import core.memory;
    GC.disable();

    import core.stdc.stdlib;
    {
        void* queueMemory = malloc(align16(TaskQueue.sizeof));
        g_queue = cast(shared TaskQueue*) align16(cast(size_t)queueMemory);
        TaskQueue.initQueue(&g_queue, -1);
    }
    atomicFence();
    (cast(uint)n_workers) = n_workers_;

    import core.stdc.stdlib;
    alloc = cast(shared) Alloc(ushort.max * 4);

    printf("starting %d workers\n", n_workers);
    g_workers.length = n_workers;

    import core.stdc.stdio;
    import std.parallelism : totalCPUs;
    printf("Found %d cores\n", totalCPUs);

    void* queuesMem = malloc(align16(((TaskQueue*).sizeof * g_workers.length)));
    g_queues = (cast(shared(TaskQueue*)*)align16(cast(size_t)queuesMem))[0 .. g_workers.length];

    atomicFence();

    {
        mixin(zoneMixin("Thread creation"));
        foreach(i; 0 .. g_workers.length)
        {
            g_workers[i] = cast(shared) Worker(new Thread(&workerFunction));
        }
    }

    printf("All worker threads are created\n");

    {
        mixin(zoneMixin("threadStartup"));
        foreach(i; 0 .. g_workers.length)
        {
            (cast()g_workers[i].workerThread).start();
        }
    }

    // we need to wait until all the threads had a chance to init their queues
    wait_until_workers_are_ready();

    // printf ("workers are ready it seems\n");
    // fire up the watcher which terminates the threads
    // before we push tasks since it also reports stats
    auto watcher = new Thread(&watcherFunction, 128);

    WorkersQueuesAndWatcher result =
    {
        queues : g_queues,
        watcher : cast(shared)watcher,
        killTheWatcher : &killTheWatcher,
        killTheWorkers : &killTheWorkers,
        workers : g_workers,
    };

    return result;
}


version (MARS) {}
else
{
    void main(string[] args)
    {
        import core.stdc.stdio;

        int n_workers_;
        if (args.length == 2 && args[1].length && args[1].length < 3)
        {
            if (args[1].length == 2)
                n_workers_ += ((args[1][0] - '0') * 10);
            n_workers_ += (args[1][$-1] - '0');
        }
        import std.parallelism : totalCPUs;

        if (!n_workers_)
            n_workers_ = totalCPUs - 1;

        auto workersAndQueues = fluffy_get_queues(n_workers_);
        (cast()workersAndQueues.watcher).start(); // first start the watcher!

        shared ulong sum;
        shared TicketCounter sumSync;
        auto counterTask = Task(&countTaskFn, cast(shared void*)&sum, &sumSync);
        enum task_multiplier = 96;

        WorkMarkerArgs workMarkerArgs = { work : &counterTask, how_many : task_multiplier };

        // now we can push the work!
        auto workMaker = Task(&workMakerFn, cast(shared void*)&workMarkerArgs);
        printf("sum before addnig tasks: %llu\n", sum);
        enum main_task_issues = 32;
        atomicStore(expected_completions, (task_multiplier * main_task_issues) + main_task_issues);

        foreach(_;0 .. main_task_issues)
        {
            // we need to loop on addTask because we might haven't got the chacne to schdule it
            while(!addTask(&workMaker))
            {
                mixin(zoneMixin("waiting for queue to empty"));
                micro_sleep(1);
            }
        }
        micro_sleep(70);
        const expected = cast(ulong) (10_000 * main_task_issues * task_multiplier);
        printf("expected: %llu\n", expected);

        ulong lastSum;

        while((lastSum = atomicLoad(sum)) != expected)
        {
            micro_sleep(310);
            //printf("lastSum: %llu\n", lastSum);
        }

        foreach(ref w;workersAndQueues.workers)
        {
            (cast()w.workerThread).join();
        }

        printf("sum: %llu\n", sum);
    }
}

struct WorkMarkerArgs
{
    Task* work;
    uint how_many;
}

@Task void workMakerFn(Task* task)
{
    TracyMessage("workMakerFn");

    auto args = cast(WorkMarkerArgs*) task.taskData;

    foreach(_; 0 .. args.how_many)
    {
        while(!addTask(args.work))
        {
            TracyMessage("work_maker_yield");
            task.currentFiber.yield();
            TracyMessage("work_maker_continue");
        }
    }
    // printf("WorkMaker done\n");
}

@Task void countTaskFn(Task* task)
{
    with (task)
    {
        int x = 0;
        while(++x != 10_000) {}


        if (!syncLock)
        {
            assert(0, "The countTask needs a syncLock! since it has shared result");
        }
        const syncResultTicket = syncLock.drawTicket();

        {
            mixin(zoneMixin("waiting on result sync"));
            while(!syncLock.servingMe(syncResultTicket)) {}
            atomicFence();
        }

        {
            // not shared because we aquired the lock
            auto sumP = cast(ulong*) task.taskData;
            (*sumP) += x;
        }

        {
            atomicFence();
            syncLock.releaseTicket(syncResultTicket);
        }
    }
}
