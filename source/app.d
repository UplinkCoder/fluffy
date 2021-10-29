import core.memory : GC;
import fluffy.taskfiber;
import fluffy.ticket;
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

struct TaskInQueue
{
    Task* taskP;
    uint queueIndex;
}

extern (C) void breakpoint () {}

uint addTask(Task* task, uint myQueue = uint.max)
{
    mixin(zoneMixin("addTask"));

    /*tls*/ static currentQueue = 0;
    uint pushIntoQueue = currentQueue;

    static immutable queueCutOff = cast(int) (TaskQueue.init.queue.length * (5f/6f));
    if (myQueue != uint.max && queues[myQueue].tasksInQueue() < queueCutOff)
    {
        pushIntoQueue = myQueue;
    }

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

        if (++currentQueue >= queues.length)
        {
            currentQueue = 0;
        }

        return queues[pushIntoQueue].push(task);
    }
}

align(16) struct TaskQueue {
    align (16) shared TicketCounter queueLock;

    align(16) shared uint readPointer; // head
    align(16) shared uint writePointer; // tail

    Task[1024] queue;

    short queueID;

    /// returns how many tasks have been stolen
    /// this function will deposit the stolen items directly
    /// into your queue
    /// we will lock it for this purpose
    int steal(int stealAmount, shared(TaskQueue)* thiefQueue, Ticket ticket) shared
    {
        // we can assume the thief has locked the queue;
        // let's make sure though
        if (queueLock.currentlyServing != ticket.ticket)
        {
            printf("queueLock not held by theif? -- thiefTicket: %d -- currentlyServing: %d",
                ticket.ticket, queueLock.currentlyServing);
            assert(0);
        }


        import std.algorithm.comparison : min;

        int stolen_items;
        atomicFence!(MemoryOrder.seq)();
        {
            // we are locked so raw reads are fine
            const victimReadP = atomicLoad!(MemoryOrder.raw)(readPointer) & (queue.length - 1);
            const victimWriteP = atomicLoad!(MemoryOrder.raw)(writePointer) & (queue.length - 1);
            stolen_items = min(stealAmount, tasksInQueue(victimReadP, victimWriteP));
            breakpoint;
            if (victimReadP <= victimWriteP // writeP - readP = items ok
                || victimWriteP >= stolen_items // ignore wraparound if we don't steal across the boundry
            )
            {
                // easy case we can just substract to get the number of items
                auto begin_pos = cast(int) (victimWriteP - stolen_items);
                int pushed = thiefQueue.push(cast(Task*)&((queue)[begin_pos]), cast(int)(victimWriteP - begin_pos));
                uint newWritePointer = cast(uint)(victimWriteP - pushed);
                stolen_items = pushed;
                // stealing renormalizes or pointers ... nice
                atomicStore!(MemoryOrder.raw)(writePointer, newWritePointer);
            }
            else
            {
                // not as easy we need to push in two steps
                // first from writePointer to zero
                int remaining = cast(int)(stolen_items - victimWriteP);
                int newWritePointer = cast(int)(queue.length - remaining);
                int pushed = thiefQueue.push(cast(Task*)&((queue)[0]), victimWriteP);
                // we didn't lock the queue when we initiated the steal ... so maybe be could not actually push our stolen items
                
                if (stolen_items - pushed > remaining)
                {
                    // we couldn't push all of them
                    // the number of stolen items if the number of what we could push
                    newWritePointer = cast(uint)(victimWriteP - pushed);
                    stolen_items = pushed;
                }
                else
                {
                    pushed = thiefQueue.push(cast(Task*)&((queue)[newWritePointer]), remaining);
                    newWritePointer = cast(uint)(queue.length - pushed);
                    stolen_items = (stolen_items - remaining + pushed);   
                }
                atomicStore!(MemoryOrder.raw)(writePointer, newWritePointer);
            }
            
        }

        return stolen_items;
    }

    unittest
    {
        TaskQueue victim;
        // victim.push()
        TaskQueue thief;
    }

    bool isLocked()
    {
        return queueLock.nextTicket != queueLock.currentlyServing;
    }

    static int tasksInQueue(uint readP, uint writeP)
    {
        pragma(inline, true);
        if (writeP >= readP)
        {
            return cast(int)(writeP - readP);
        }
        else
        {
            // wrap-around
            // we go from readP to length and from zero to writeP
            return cast(int)((TaskQueue.init.queue.length - readP) + writeP); 
        }
    }

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
        return tasksInQueue(readP, writeP);
    }

    static void initQueue(shared(TaskQueue*)* q, short queueID)
    {
        import core.stdc.stdlib;
        void* queueMem = malloc(align16(TaskQueue.sizeof + 16));
        (*q) = (cast(shared(TaskQueue)*)align16(cast(size_t)queueMem));

        (**q).readPointer = (**q).writePointer = 0;
        (**q).queueLock = TicketCounter.init;
        (**q).queueID = queueID;
    }

    uint enqueueTermination(string terminationMessage) shared
    {
        // little guard to we don't push the message if the chance of success is low
        if (tasksInQueue() > (queue.length - 4))
            return false;

        auto terminationTask = Task(terminationDg, cast(shared void*) pushString(terminationMessage));
        return push(&terminationTask);
    }

    uint push(Task* task, int n = 1) shared
    {
        mixin(zoneMixin("push"));
        uint tasks_written = 0;
        // as an optimisation we check for an full queue first
        {
            const readP = atomicLoad!(MemoryOrder.raw)(readPointer) & (queue.length - 1);
            const writeP = atomicLoad!(MemoryOrder.raw)(writePointer) & (queue.length - 1);
            // printf("before pull -- readP: %d, writeP: %d\n", readP, writeP);
            // update readP and writeP
            if (readP == writeP + 1)
            {
                return 0;
            }
        }

/+
        if (queueLock.apporxQueueLength > 7)
        {
            return false;
        }
+/

        Ticket ticket;
        {
            ticket = queueLock.drawTicket();
        }


        {
            // mixin(zoneMixin("waiting"));
            while(!queueLock.servingMe(ticket)) {}
            atomicFence!(MemoryOrder.seq);
        }
        // we've got the lock
        //printf("push Task\n");
        // only release a ticket which you have aquired
        scope (exit) queueLock.releaseTicket(ticket);
        {
            const readP = atomicLoad(readPointer) & (queue.length - 1);
            const writeP = atomicLoad(writePointer) & (queue.length - 1);
            // update readP and writeP

            if (readP == writeP + 1 || // queue is full
                tasksInQueue(readP, writeP) + n >= queue.length)
            {
                // tests don't fit.
                return 0;
            }

            // we know the tasks fit so there's no problem with us just updating
            // the write pointer here we have the old value if writeP
            atomicOp!"+="(writePointer, n);
            {
                {
                    foreach(tIdx; 0 .. n)
                    {
                        task[tIdx].queueID = queueID;
                        task[tIdx].schedulerId = atomicOp!"+="(task.runningSchedulerId, 1);
                    }
                }
                atomicFence!(MemoryOrder.seq);
                {
                    // let's do the simple case first
                    if (writeP + n <= queue.length)
                    {
                        queue[writeP .. writeP + n] = task[0 .. n];
                    }
                    else
                    {
                        int overhang = cast(int)((writeP + n) - queue.length);
                        // this is how much we cannot fit
                        // therefore n - overhang is how much we can fit at the end
                        int first_part = n - overhang;


                        queue[writeP .. $] = task[0 .. first_part];
                        queue[0 .. n - first_part] = task[first_part .. n];
                    }
                }
                atomicFence!(MemoryOrder.seq);
            }
        }

        return n;
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
                return false;
            }
            // printf("pulled task from queue\n");

            *task = cast()(queue)[readP];
            atomicFence!(MemoryOrder.seq)();

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

shared TaskQueue*[] queues;


struct Worker
{
    Thread workerThread;
    shared bool terminate;
    FiberPool workerFiberPool;
}

shared Worker[] workers;
extern (C) void ___tracy_set_thread_name( const char* name );

enum threadproc;

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
shared bool killTheWatcher = false;
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
        printf("lastCompletedTasks -- %llu -- expected_completions %llu", 
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

shared TicketCounter globalLock;
shared uint workersReady = 0;

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
    for(;;)
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
                printf("Queue empty ... let's steal some work\n");

                {
                    uint max_queue_length = 30; // a vicitm needs to have at least 30 tasks to be considered a target
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
                            printf("got the lock doing the steal\n");
                            mixin(zoneMixin("Stealing work"));
                            atomicFence!(MemoryOrder.seq)();
                            
                            auto n_stolen = victim.steal(steal_amount, myQueue, ticket);
                            printf("stolen %d tasks\n", n_stolen);
                            atomicFence!(MemoryOrder.seq)();
                            victim.queueLock.releaseTicket(ticket);

                        }
                        continue;
                    }
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

shared TaskQueue* g_queue;

shared(TaskQueue*[]) fluffy_get_queues(uint n_workers_)
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

    string fName = "a";
    string[] result;

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
    watcher.start();
    scope (exit)
    {
        watcher.join();
    }


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

    foreach(ref w;workers)
    {
        (cast()w.workerThread).join();
    }

    printf("sum: %llu\n", sum);

//    assert(sum == 10_000 * 12 * 1440);

    printf("completedTasks: %llu\n", atomicLoad!(MemoryOrder.raw)(completedTasks));
    return cast(shared(TaskQueue*[])) queues;
}
