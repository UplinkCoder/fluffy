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
version (tracy)
{
    import tracy;
}
else
{
    extern (C) void ___tracy_set_thread_name( const char* name ) {}
    extern (C) void ___tracy_emit_plot ( const char* name, double value ) {}
    extern (C) void ___tracy_emit_message ( const char* name, size_t length, int callstack ) {}

    void TracyMessage(string message) {}

    string zoneMixin(string zoneName)
    {
        return "";
    }
}
static  immutable task_function_t terminationDg =
    (Task*) { };


void micro_sleep(uint micros)
{
    timespec ts;
    ts.tv_nsec = micros * 1000;
    nanosleep(&ts, null);
}


struct Alloc
{
    ubyte* memPtr;
    uint capacity_remaining;
    shared TicketCounter allocLock;

    this(uint size)
    {
        import core.stdc.stdlib;
        size = cast(uint) align16(size);
        memPtr = cast(ubyte*) malloc(size);

        capacity_remaining = size;
    }

    ubyte* alloc(uint sz) shared
    {
        auto ticket = allocLock.drawTicket();
        while (!allocLock.servingMe(ticket)) {}

        atomicFence!(MemoryOrder.seq)();
        scope(exit) allocLock.releaseTicket(ticket);

        sz = cast(uint)align16(sz);
        assert(capacity_remaining > sz);
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

    char[32]* queueStringMem = cast(char[32]*)alloc.alloc(32 * cast(uint)workers.length);
    char[32][] worker_queue_strings = queueStringMem[0 .. workers.length];

    worker_queue_strings.length = workers.length;
    foreach(i, ref workerer_queue_string;worker_queue_strings)
    {
        sprintf(workerer_queue_string.ptr, "queue %d\0", cast(int) i);
    }

    TracyMessage("Watcher says Hello!");
    while(!atomicLoad(killTheWatcher))
    {
        lastCompletedTasks = atomicLoad!(MemoryOrder.raw)(completedTasks);
        ___tracy_emit_plot("completedTasks", lastCompletedTasks);
        foreach(i; 0 .. workers.length)
        {
            TaskQueue* q = cast(TaskQueue*)queues[i];
            ___tracy_emit_plot(worker_queue_strings[i].ptr, queues[i].tasksInQueue());
        }

        micro_sleep(1);
        if (lastCompletedTasks >= atomicLoad!(MemoryOrder.raw)(expected_completions))
            break;
    }
    // giving tasks 5 microseconds to take care of unfinished buissness
    micro_sleep(5);

    {
        printf("lastCompletedTasks -- %llu -- expected_completions %u",
            lastCompletedTasks, atomicLoad!(MemoryOrder.raw)(expected_completions));
        printf("watcher: enquing termination\n");
        mixin(zoneMixin("watcher: enqueueingTermnination"));
        foreach(i; 0 .. workers.length)
        {
            printf("termination for worker %d ... ", cast(int)i);
            bool enqueuedTermination = false;
            while(!enqueuedTermination)
            {
                enqueuedTermination = !!queues[i].enqueueTermination("Watcher termination");
            }
            printf("Termination scheduled\n");
        }
    }
    TracyMessage("Watcher says bye!");
}

private shared TicketCounter globalLock;
private shared uint workersReady = 0;

@threadproc void workerFunction () {
    mixin(zoneMixin("workerFunction"));

    static shared int workerCounter;
    /*tls*/ short workerIndex = cast(short)(atomicOp!"+="(workerCounter, 1) - 1);
    /*tls*/ char[16] worker_name;
    sprintf(&worker_name[0], "Worker %d", workerIndex);
    printf("%s is starting\n", &worker_name[0]);

    ___tracy_set_thread_name(&worker_name[0]);
    // printf("Startnig: %d\n", workerIndex);
    /*tls*/ shared (bool) *terminate = &workers[workerIndex].terminate;
    /*tls*/ shared(TaskQueue*)* myQueueP = &queues[workerIndex];
    TaskQueue.initQueue(myQueueP, cast(short)(workerIndex + 1));
    shared (TaskQueue)* myQueue = *myQueueP;
    /*tls*/ FiberPool* fiberPool = cast(FiberPool*)&workers[workerIndex].workerFiberPool;
    /*tls*/ int[fiberPool.fibers.length] fiberExecCount;
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
    /*tls*/ Task task;

    /*tls*/ static uint nextExecIdx;
    while(!killTheWorkers)
    {
        // mixin(zoneMixin("WorkerLoop"));
        TaskFiber execFiber;
        if (auto idx = fiberPool.nextFree())
        {
            if (myQueue.pull(&task))
            {
                if (task.fn is terminationDg)
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
                execFiber.assignTask(&task);
            }
            else if (!fiberPool.n_used)
            {
                // no fibers used
                mixin(zoneMixin("sleepnig"));
                TaskQueue* q = cast(TaskQueue*)myQueue;
                int longest_queue_idx = -1;
                enum work_stealing = true;
                static if (work_stealing)
                {
                    uint max_queue_length = 50; // a vicitm needs to have at least 100 tasks to be considered a target
                    shared(TaskQueue)* victim;
                    foreach(qIdx; 0 .. queues.length)
                    {
                        auto canidate = queues[qIdx];
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
            mixin(zoneMixin("FindNextFiber"));
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
        }
        //___tracy_emit_plot(worker_name.ptr, fiberPool.freeBitfield);
        // execute a fiber in the pool
        {
            fiberExecCount[execFiber.idx]++;
            mixin(zoneMixin("FiberExecution"));
            //printf("executing fiber: %p -- idx:%d\n", execFiber, execFiber.idx);
            //printf("stateBeforeExec: %s\n", execFiber.stateToString(execFiber.state()).ptr);
            assert(execFiber.state() == execFiber.state().HOLD, execFiber.stateToString(execFiber.state()));
            execFiber.call();
            // if this completed the fiber we need to to reset it and send it back to the pool
            if (execFiber.state() == execFiber.state().TERM)
            {
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
shared TaskQueue*[] queues;
shared Worker[] workers;

version (MARS) {}
else
{
    void main(string[] args)
    {
        
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
        auto myqueues = fluffy_get_queues(n_workers_);        
    }
}

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
    alloc = cast(shared) Alloc(ushort.max);

    printf("starting %d workers\n", n_workers);
    workers.length = n_workers;

    import core.stdc.stdio;
    import std.parallelism : totalCPUs;
    printf("Found %d cores\n", totalCPUs);

    void* queuesMem = malloc(align16(((TaskQueue*).sizeof * workers.length)));
    queues = (cast(shared(TaskQueue*)*)align16(cast(size_t)queuesMem))[0 .. workers.length];

    atomicFence();

    {
        mixin(zoneMixin("Thread creation"));
        foreach(i; 0 .. workers.length)
        {
            workers[i] = cast(shared) Worker(new Thread(&workerFunction));
        }
    }

    printf("All worker threads are created\n");

    {
        mixin(zoneMixin("threadStartup"));
        foreach(i; 0 .. workers.length)
        {
            (cast()workers[i].workerThread).start();
        }
    }

    // we need to wait until all the threads had a chance to init their queues
    wait_until_workers_are_ready();

    printf ("workers are ready it seems\n");
    // fire up the watcher which terminates the threads
    // before we push tasks since it also reports stats
    auto watcher = new Thread(&watcherFunction, 128);

    WorkersQueuesAndWatcher result =
    {
        workers : workers,
        queues : queues,
        watcher : watcher,
        killTheWatcher = &killTheWatcher,
        killTheWorkers : &killTheWorkers
    };

    return result;
}
