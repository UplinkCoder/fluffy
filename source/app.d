import core.memory : GC;
import fluffy.taskfiber;
import fluffy.ticket;
import core.thread;
import core.atomic;
import core.stdc.stdio;
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

extern (C) void breakpoint () {}

bool addTask(Task* task)
{
    mixin(zoneMixin("addTask"));
    static currentQueue = 0;

    version (multi_try)
    {
        auto maxAttempts = queues.length;

        bool succeses = false;
        while(maxAttempts-- && !succeses)
        {
            succeses = queues[currentQueue].push(task);
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

        return queues[currentQueue].push(task);
    }
}

align(16) struct TaskQueue {
    align (16) shared TicketCounter queueLock;

    align(16) shared uint readPointer; // head
    align(16) shared uint writePointer; // tail

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
        if (writeP >= readP)
        {
            return cast(int)(writeP - readP);
        }
        else
        {
            // wrap-around
            // we go from readP to length and from zero to writeP
            return cast(int)((queue.length - readP) + writeP); 
        }
    }

    void initQueue() shared
    {
        import core.stdc.stdlib;
        readPointer = writePointer = 0;
        queueLock = TicketCounter.init;
        void* taskMemPtr = malloc(align16((*queue).sizeof));
        queue = cast(typeof(queue))align16(cast(size_t)taskMemPtr);
    }

    bool enqueueTermination(string terminationMessage) shared
    {
        // little guard to we don't push the message if the chance of success is low
        if (tasksInQueue() > (queue.length - 4))
            return false;

        auto terminationTask = Task(terminationDg, cast(shared void*) pushString(terminationMessage));
        return push(&terminationTask);
    }

    bool push(Task* task, int n = 1) shared
    {
        mixin(zoneMixin("push"));
        if (task.fn is terminationDg)
        {
            printf("pushing termination\n");
        }
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
            atomicFence();
        }
        // we've got the lock
        //printf("push Task\n");
        scope (exit) queueLock.releaseTicket(ticket);
        {
            const readP = atomicLoad(readPointer) & (queue.length - 1);
            const writeP = atomicLoad(writePointer) & (queue.length - 1);
            // update readP and writeP
            auto tasksFit = queue.length - (cast(int)(writeP - readP));
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
        if (task.fn is terminationDg)
        {
            printf("Success .... termination accepted\n");
            breakpoint;
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
            atomicFence!();
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
    shared bool terminate;
    FiberPool workerFiberPool;
}

shared Worker[] workers;
extern (C) void ___tracy_set_thread_name( const char* name );

enum threadproc;

void sumTask(shared (ulong)* arg, shared (TicketCounter*) argLock)
{
    const argTicket = argLock.drawTicket();
    {
        while(!argLock.servingMe(argTicket)) {}
        atomicFence();
    }

    {
        arg += 10_000;
    }

    {
        atomicFence();
        argLock.releaseTicket(argTicket);
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
shared bool killTheWatcher = false;
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
        ___tracy_emit_plot("completedTasks", lastCompletedTasks);
        foreach(i; 0 .. workers.length)
        {
            ___tracy_emit_plot(worker_queue_strings[i].ptr, queues[i].tasksInQueue);
        }

        Thread.sleep(1.usecs);
    }
    // you have half a second.
    {
        printf("watcher: enquing termination\n");
        mixin(zoneMixin("watcher: enqueueingTermnination"));
        foreach(i; 0 .. workers.length)
        {
            printf("termination for worker %d ... ", cast(int)i);
            bool enqueuedTermination = false;
            while(!enqueuedTermination)
            {
                enqueuedTermination = queues[i].enqueueTermination("Watcher termination");
            }
            printf("Termination scheduled\n");
        }
    }
    TracyMessage("Watcher says bye!");
}

shared TicketCounter globalLock;

@threadproc void workerFunction () {
    mixin(zoneMixin("workerFunction"));

    static shared int workerCounter;
    /*tls*/ int workerIndex = atomicOp!"+="(workerCounter, 1) - 1;
    /*tls*/ char[16] worker_name;
    sprintf(&worker_name[0], "Worker %d", workerIndex);
    ___tracy_set_thread_name(&worker_name[0]);
    // printf("Startnig: %d\n", workerIndex);
    /*tls*/ shared (bool) *terminate = &workers[workerIndex].terminate;
    /*tls*/ shared(TaskQueue)* myQueue = &queues[workerIndex];
    /*tls*/ FiberPool* fiberPool = cast(FiberPool*)&workers[workerIndex].workerFiberPool;
    /*tls*/ int[fiberPool.fibers.length] fiberExecCount;
    {
        auto initTicket = globalLock.drawTicket();

        {
            while (!globalLock.servingMe(initTicket)) {}
            atomicFence();
        }
        /*tls*/ fiberPool.initFiberPool();
        {
            atomicFence();
            globalLock.releaseTicket(initTicket);
        }
    }


    /*tls*/ int myCounter = 0;
    /*tls*/ Task task;

    /*tls*/ static uint nextExecIdx;
    while(!atomicLoad!(MemoryOrder.raw)(*terminate))
    {
        // mixin(zoneMixin("WorkerLoop"));
        TaskFiber execFiber;
        if (auto idx = fiberPool.nextFree())
        {
            if ((myQueue.pull(&task)))
            {
                if (task.fn is terminationDg)
                {
                    auto terminationMessage = cast(string*) task.taskData;
                    ___tracy_emit_message("recieved termination signal", "recieved termination signal".length, 0);
                    TracyMessage(*terminationMessage);
                    foreach(fIdx; 0 .. fiberPool.fibers.length)
                    {
                        printf("fiber %d -- exeCount: %d\n", cast(int) fIdx, fiberExecCount[fIdx]);
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
                // printf("Queue empty ... let's sleep\n");
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
    alloc = cast(shared) Alloc(ushort.max);

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

    printf("All workers are initialized\n");

    string fName = "a";
    string[] result;
    // addTask(Task(&loadFiles, cast(shared void*)&fName, false));
    // printf("tasksInQueue zero: %d\n", queues[0].tasksInQueue());
    // Thread.sleep(msecs(100));

    // task should now be in queue zero
    // printf("tasksInQueue zero: %d\n", queues[0].tasksInQueue());

    // all need be initted before we start them
    {
        mixin(zoneMixin("threadStartup"));
        foreach(i; 0 .. workers.length)
        {
            (cast()workers[i].workerThread).start();
        }
    }

    // fire up the watcher which terminates the threads
    // before we push tasks since it also reports stats
    auto watcher = new Thread(&watcherFunction, 128);
    watcher.start();
    scope (exit)
    {
        watcher.join();
    }


    shared ulong sum;
    shared TicketCounter sumSync;
    auto counterTask = Task(&countTaskFn, cast(shared void*)&sum, &sumSync);
    WorkMarkerArgs workMarkerArgs = { work : &counterTask, how_many : 1024 };

    // now we can push the work!
    auto workMaker = Task(&workMakerFn, cast(shared void*)&workMarkerArgs);
    printf("sum before addnig tasks: %llu\n", sum);

    foreach(_;0 .. 32)
    {
        // we need to loop on addTask because we might haven't got the chacne to schdule it
        while(!addTask(&workMaker))
        {
            mixin(zoneMixin("waiting for queue to empty"));
            Thread.sleep(1.usecs);
        }
    }

    {
        mixin(zoneMixin("Worker-Time"));
        foreach(i; 0 .. workers.length)
        {
            Thread.sleep(200.msecs);
        }

        atomicStore(killTheWatcher, true);
    }

    printf("sum: %llu\n", sum);
    printf("expected: %llu\n", cast(ulong) (10_000 * 32 * 32));

//    assert(sum == 10_000 * 32 * 32);

    printf("completedTasks: %llu\n", completedTasks);
}
