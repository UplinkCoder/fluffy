import core.memory : GC;
import fluffy.taskfiber;
import fluffy.ticket;
import core.thread;
import core.atomic;
import core.stdc.stdio;
static if (0)
{
    import tracy;
}
else
{
    extern (C) void ___tracy_set_thread_name( const char* name ) {}
    extern (C) void ___tracy_emit_plot ( const char* name, double value ) {}


    string zoneMixin(string zoneName)
    {
        return "";
    }
}
static  immutable task_function_t terminationDg =
    (Task*) { };


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
        scope(exit) allocLock.releaseTicket(ticket);

        sz = cast(uint)align16(sz);
        assert(capacity_remaining > sz);
        ubyte* result = cast(ubyte*)(memPtr + sz);
        (cast()memPtr) += sz;
        (cast()capacity_remaining) -= sz;
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

struct TaskInQueue
{
    Task* taskP;
    uint queueIndex;
}

bool addTask(Task task)
{
    static currentQueue = 0;

    version (multi_try)
    {
        auto maxAttempts = queues.length;

        bool succeses = false;
        while(maxAttempts-- && !succeses)
        {
            succeses = queues[currentQueue].push(&task);
            if (++currentQueue >= queues.length)
                currentQueue = 0;
        }

        return succeses;
    }
    else
    {
        scope(exit)
        {
            if (++currentQueue >= queues.length)
                currentQueue = 0;
        }

        return queues[currentQueue].push(&task);
    }
}

align(16) struct TaskQueue {
    align (16) shared TicketCounter queueLock;

    align(16) shared int readPointer; // head
    align(16) shared int writePointer; // tail

    Task[1024]* queue;
    /// a read pointer of -1 signifies the queue is empty


    int tasksInQueue(bool consistent = false) shared
    {
        Ticket ticket;
        if (consistent)
        {
            ticket = queueLock.drawTicket();
            while (!queueLock.servingMe(ticket)) {}
            atomicFence();
        }
        scope(exit)
        {
            if (consistent) queueLock.releaseTicket(ticket);
        }
        const readP = atomicLoad!(MemoryOrder.raw)(readPointer) & (queue.length - 1);
        const writeP = atomicLoad!(MemoryOrder.raw)(writePointer) & (queue.length - 1);
        return cast(int)(writeP - readP);


    }

    void initQueue() shared
    {
        import core.stdc.stdlib;
        readPointer = writePointer = 0;
        queueLock = TicketCounter.init;
        void* taskMemPtr = malloc(align16((*queue).sizeof));
        queue = cast(typeof(queue))align16(cast(size_t)taskMemPtr);
    }

    bool enqueueTermination() shared
    {
        auto terminationTask = Task(terminationDg);
        return push(&terminationTask);
    }

    bool push(Task* task, int n = 1) shared
    {
        mixin(zoneMixin("push"));

        // as an optimisation we check for an full queue first
        {
            const readP = atomicLoad!(MemoryOrder.raw)(readPointer) & (queue.length - 1);
            const writeP = atomicLoad!(MemoryOrder.raw)(writePointer) & (queue.length - 1);
            // printf("before pull -- readP: %d, writeP: %d\n", readP, writeP);
            // update readP and writeP
            if (readP == writeP + 1)
            {
                return false;
            }
        }

        Ticket ticket;
        {
            ticket = queueLock.drawTicket();
        }

        {
            mixin(zoneMixin("waiting"));
            while(!queueLock.servingMe(ticket)) {}
        }
        // we've got the lock
        //printf("push Task\n");
        scope (exit) queueLock.releaseTicket(ticket);
        {
            const readP = atomicLoad(readPointer) & (queue.length - 1);
            const writeP = atomicLoad(writePointer) & (queue.length - 1);
            // update readP and writeP
            auto tasksFit = queue.length - tasksInQueue();
            if (n > queue.length / 2)
            {
                assert(0, "this many tasks can never fit");
            }
            if (n >= tasksFit)
            {
                return false;
            }

            if (readP == writeP + 1)
            {
                return false;
            }

            {
                cast()(*queue)[writeP] = *task;
                atomicFence!(MemoryOrder.seq)();
            }

            {
                atomicOp!"+="(writePointer, 1);
            }
        }

        return true;
    }

    bool pull(Task* task) shared
    {
        mixin(zoneMixin("Pull"));
        // printf("try pulling\n");
        // as an optimisation we check for an empty queue first
        {
            const readP = atomicLoad!(MemoryOrder.raw)(readPointer) & (queue.length - 1);
            const writeP = atomicLoad!(MemoryOrder.raw)(writePointer) & (queue.length - 1);
            // printf("before pull -- readP: %d, writeP: %d\n", readP, writeP);
            // update readP and writeP
            if (readP == writeP)
            {
                return false;
            }
        }
        // the optimisation is totally worth it

        Ticket ticket;
        {
            //mixin(zoneMixin("drawing ticket"));
            ticket = queueLock.drawTicket();
        }
        {
            //mixin(zoneMixin("wating on mutex"));
            while(!queueLock.servingMe(ticket)) {}
        }

        {
            scope (exit) queueLock.releaseTicket(ticket);
            const readP = atomicLoad(readPointer) & (queue.length - 1);
            const writeP = atomicLoad(writePointer) & (queue.length - 1);
            // printf("before pull -- readP: %d, writeP: %d\n", readP, writeP);
            // update readP and writeP
            if (readP == writeP)
            {
              //  assert(tasksInQueue() == 0);
                return false;
            }
            mixin(zoneMixin("Read"));
            // printf("pulled task from queue\n");
            atomicFence!(MemoryOrder.seq)();
            *task = cast()(*queue)[readP];

            atomicOp!"+="(readPointer, 1);
        }
        return true;
    }
}

unittest
{
    import core.stdc.stdio;
    auto t1 = Task();
    auto t2 = Task();
    auto q = TaskQueue();
    assert(q.tasksInQueue() == 0);
    auto ticket1 = q.queueLock.drawTicket();
    q.push(&t1, ticket1);
    q.queueLock.releaseTicket(ticket1);
    assert(q.tasksInQueue == 1);
    auto ticket2 = q.queueLock.drawTicket();

    q.push(&t2, ticket2);
    assert(q.tasksInQueue() == 2);
    q.queueLock.releaseTicket(ticket2);

    auto ticket3 = q.queueLock.drawTicket();
    q.pull(&t1, ticket3);
    assert(q.tasksInQueue() == 1);
    q.queueLock.releaseTicket(ticket3);
}

shared TaskQueue[] queues = void;


struct Worker
{
    Thread workerThread;
    FiberPool workerFiberPool;
}

shared Worker[] workers;
extern (C) void ___tracy_set_thread_name( const char* name );

enum threadproc;

@threadproc void watcherFunction ()
{
    // who watches the watchman
    ___tracy_set_thread_name(`Watcher`);
    mixin(zoneMixin("watcherTime"));

    uint lastCompletedTasks;
    uint no_progress;

    while((lastCompletedTasks = atomicLoad!(MemoryOrder.raw)(completedTasks)) < 10_000)
    {
        ___tracy_emit_plot("completedTasks", lastCompletedTasks);
        Thread.sleep(1.usecs);
        if (lastCompletedTasks == atomicLoad!(MemoryOrder.raw)(completedTasks))
        {
            if (no_progress++ > 1000)
            {
                // if the above is true we went an entire millisecond without making any progress
                printf("Aborting due to lack of progress\n");
                break;
            }
        }
        else
            no_progress = 0;
    }
    // you have half a second.
    {
        mixin(zoneMixin("enqueueingTermnination"));
        foreach(i; 0 .. workers.length)
        {
            bool enqueuedTermination = false;
            while(!enqueuedTermination)
            {
                enqueuedTermination = queues[i].enqueueTermination();
            };
        }
    }
}

shared TicketCounter globalLock;

@threadproc void workerFunction () {
    static shared int workerCounter;
    /*tls*/ int workerIndex = atomicOp!"+="(workerCounter, 1) - 1;
    /*tls*/ char[16] worker_name;
    sprintf(&worker_name[0], "Worker %d", workerIndex);
    ___tracy_set_thread_name(&worker_name[0]);
    // printf("Startnig: %d\n", workerIndex);
    /*tls*/ shared(TaskQueue)* myQueue = &queues[workerIndex];
    /*tls*/ FiberPool* fiberPool = cast(FiberPool*)&workers[workerIndex].workerFiberPool;
    {
        auto initTicket = globalLock.drawTicket();

        atomicFence();
        while (!globalLock.servingMe(initTicket)) {}

        atomicFence();
        /*tls*/ fiberPool.initFiberPool();

        atomicFence();
        globalLock.releaseTicket(initTicket);
    }



    /*tls*/ bool terminate = false;
    /*tls*/ int myCounter = 0;
    /*tls*/ Task task;

    /*tls*/ static uint nextExecIdx;
    while(!terminate)
    {
        mixin(zoneMixin("WorkerLoop"));
        TaskFiber execFiber;
        if (auto idx = fiberPool.nextFree())
        {
            if ((myQueue.pull(&task)))
            {
                if (task.fn is terminationDg)
                {
                    terminate = true;
                    // TracyMessage("recieved termination signal");
                }

                execFiber = fiberPool.getNextFree();
                execFiber.assignTask(&task);
                //task.result = task.fn(task.taskData);
            }
            else if (!fiberPool.n_used)
            {
                // no fibers used
                mixin(zoneMixin("sleepnig"));
                Thread.sleep(1.usecs);
                continue;
            }
        }

        if (!execFiber)
        {
            mixin(zoneMixin("FindNextFiber"));
            //printf("no new task ... searching for fiber to exec -- %llx\n", fiberPool.freeBitfield);
            // if we didn't add a task just now chose a random fiber to exec
            const nonFree = ~fiberPool.freeBitfield;
            ulong nextExecMask;
            auto localNextIdx = nextExecIdx & 63;
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
                localNextIdx = (++localNextIdx & 63);
            }
        }
        //___tracy_emit_plot(worker_name.ptr, fiberPool.freeBitfield);
        // execute a fiber in the pool
        {
            mixin(zoneMixin("FiberExecution"));
            //printf("executing fiber: %p -- idx:%d\n", execFiber, execFiber.idx);
            //printf("stateBeforeExec: %s\n", execFiber.stateToString(execFiber.state()).ptr);
            assert(execFiber.state() == execFiber.state().HOLD, execFiber.stateToString(execFiber.state()));
            execFiber.call();
            // if this completed the fiber we need to to reset it and send it back to the pool
            if (execFiber.state() == execFiber.state().TERM)
            {
                // printf("We just completed a task on fiberIdx: %p\n", &execFiber.idx);
                atomicOp!"+="(completedTasks, 1);
                execFiber.hasTask = false;
                printf("freeing fiber\n");
                fiberPool.free(&execFiber);
                execFiber.reset();

            }
        }
    }
}
shared int completedTasks;

version (MARS) {}
else
{
    void main(string[] args)
    {
        return fluffy_main(args);
    }
}

void  fluffy_main(string[] args)
{
    mixin(zoneMixin("Main"));

    import core.memory;
    GC.disable();

    import std.parallelism : totalCPUs;
    import core.stdc.stdlib;
    alloc = cast(shared) Alloc(ushort.max * ushort.max);

    int n_workers;
//    workers.length = totalCPUs - 1;
    if (args.length == 2 && args[1].length && args[1].length < 3)
    {
        n_workers = 0;
        if (args[1].length == 2)
            n_workers += ((args[1][0] - '0') * 10);
        n_workers += (args[1][$-1] - '0');
    }

    if (!n_workers)
        n_workers = totalCPUs - 1;

    printf("starting %d workers\n", n_workers);
    workers.length = n_workers;

    void* queueMemory = malloc(align16(TaskQueue.sizeof * workers.length));
    shared(TaskQueue)* alignedMem = cast(shared TaskQueue*) align16(cast(size_t)queueMemory);
    pragma(msg, TaskQueue.sizeof);
    queues = alignedMem[0 .. workers.length];

    import core.stdc.stdio;

    printf("Found %d cores\n", totalCPUs);

    {
        foreach(i; 0 .. workers.length)
        {
            queues[i].initQueue();
        }
    }

    {
        mixin(zoneMixin("Thread creation"));
        foreach(i; 0 .. workers.length)
        {
            workers[i] = cast(shared) Worker(new Thread(&workerFunction));
        }
    }


    string fName = "a";
    string[] result;
    // addTask(Task(&loadFiles, cast(shared void*)&fName, false));
    // printf("tasksInQueue zero: %d\n", queues[0].tasksInQueue());
    // Thread.sleep(msecs(100));

    // task should now be in queue zero
    // printf("tasksInQueue zero: %d\n", queues[0].tasksInQueue());

    printf("All workers are initialized\n");
    // all need be initted before we start them
    {
        mixin(zoneMixin("threadStartup"));
        foreach(i; 0 .. workers.length)
        {
            (cast()workers[i].workerThread).start();
        }
    }
    // fire up the watcher which terminates the threads
    auto watcher = new Thread(&watcherFunction, 128);
    watcher.start();

    {
        mixin(zoneMixin("Worker-Time"));
        foreach(i; 0 .. workers.length)
        {
            (cast()workers[i].workerThread).join();
        }
    }
    watcher.join();
    printf("completedTasks: %d\n", completedTasks);
}
