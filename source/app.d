

import core.memory : GC;
import fluffy.taskfiber;
import fluffy.ticket;
import core.thread;
import core.atomic;
import core.stdc.stdio;
//import tracy;

string zoneMixin(string zoneName)
{
    return "";
}

alias task_dg_t = shared (void*) function (shared void*);

static  immutable task_dg_t terminationDg =
    (shared void*) { return cast(shared void*)null; };


struct Alloc
{
    ubyte* memPtr;
    uint capacity_remaining;
    TicketCounter allocLock;

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

shared(void*) loadFiles(shared void* arg)
{
    string* fArg = cast(string*) arg;
    printf("Executing load files: %s\n", (*fArg).ptr);
    import std.file;
    import std.algorithm;

    if (fArg)
    {
        string[] words;
        string text = null;
        if (exists(*fArg))
        {
            printf("loading: %s\n", (*fArg).ptr);
            text = cast(string) read(*fArg);
            // printf("loading complete, text: %s\n", text.ptr);
            foreach(line;text.splitter('\n'))
            {
                bool loadNext = false;
                foreach(word;line.splitter(' '))
                {
                    if (word == "import")
                    {
                        assert(!loadNext, "import following import is forbidden");
                        loadNext = true;
                    }
                    else if (loadNext)
                    {
                        loadNext = false;
                        if (exists(word)) // if we get the name of a anoter file spwan a new file loader
                        {
                            printf("got import: %.*s\n", cast(int) word.length, word.ptr);
                            string[] sResult;
                            auto ptr = cast(shared void*) pushString(word);
                            addTask(Task(&loadFiles, ptr, false, cast(shared void*)&sResult));
                        }
                    }
                    else
                    {
                        // we got a word.
                        printf("word: %.*s\n", cast(int)word.length, word.ptr);
                    }
                }
            }
        }
    }
    return null;
}

struct TaskInQueue
{
    Task* taskP;
    uint queueIndex;
}

bool addTask(Task task)
{
/+
    Task* taskP = cast(Task*) alloc.alloc(Task.sizeof);
+/


    static currentQueue = 0;
    scope(exit)
    {
        if (++currentQueue >= queues.length)
            currentQueue = 0;
    }

    return (queues[currentQueue].push(&task));
/+
    {

        result = //TaskInQueue(TaskP, currentQueue);
    }

    return result;
+/
}

align(16) struct TaskQueue {
    align (16) shared TicketCounter queueLock;

    align(16) shared int readPointer; // head
    align(16) shared int writePointer; // tail

    Task[1024] queue;
    /// a read pointer of -1 signifies the queue is empty


    int tasksInQueue() shared
    {
        const readP = atomicLoad!(MemoryOrder.raw)(readPointer) & (queue.length - 1);
        const writeP = atomicLoad!(MemoryOrder.raw)(writePointer) & (queue.length - 1);
        return cast(int)(writeP - readP);
    }

    void initQueue() shared
    {
        readPointer = writePointer = 0;
        queueLock = TicketCounter.init;
    }

    bool enqueueTermination() shared
    {
        auto terminationTask = Task(terminationDg);
        return push(&terminationTask);
    }

    bool push(Task* task, int n = 1) shared
    {
        mixin(zoneMixin("push"));
        Ticket ticket;
        {
            ticket = queueLock.drawTicket();
        }

        {
            mixin(zoneMixin("waiting"));
            while(!queueLock.servingMe(ticket)) {}
        }
        // we've got the lock
        printf("push Task\n");
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
                cast()queue[writeP] = *task;
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
            printf("pulled task from queue\n");
            atomicFence!(MemoryOrder.seq)();
            *task = cast()queue[readP];

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



void watcherFunction ()
{
    // who watches the watchman
    // ___tracy_set_thread_name(`Watcher`);
    
}

void workerFunction () {
    static shared int workerCounter;
    int workerIndex = atomicOp!"+="(workerCounter, 1) - 1;
    char[16] worker_name;
    sprintf(&worker_name[0], "Worker %d", workerIndex);
    // ___tracy_set_thread_name(&worker_name[0]);
    // printf("Startnig: %d\n", workerIndex);
    shared(TaskQueue)* myQueue = &queues[workerIndex];
    FiberPool* fiberPool = cast(FiberPool*)&workers[workerIndex].workerFiberPool;
    fiberPool.initFiberPool();
    bool terminate = false;
    int myCounter = 0;
    Task task;
    while(!terminate)
    {
        if (auto idx = fiberPool.nextFree())
        {
            if ((myQueue.pull(&task)))
            {
                if (task.fn is terminationDg)
                {
                    terminate = true;
                    // TracyMessage("recieved termination signal");
                }

                task.result = task.fn(task.taskData);
            }
            else if (!fiberPool.n_used)
            {
                Thread.sleep(1.usecs);
            }
        }
        else
        {
            // the fiber pool is full
            // which means 
        }
        import core.stdc.stdio;
        if (myCounter++ || terminate)
        {
            int target = cast(int) (workerIndex ? workerIndex - 1 : workers.length - 1);
            // printf("[%d] sending termination signal to [%d]\n", workerIndex, target);
            while (!workers[target].workerThread) {} // wait for target to be born

            // queues[target].enqueueTermination(); // kill target
        }

    }
}
void main()
{
    mixin(zoneMixin("Main"));

    import core.memory;
    GC.disable();

    import std.parallelism : totalCPUs;
    import core.stdc.stdlib;
    alloc = cast(shared) Alloc(ushort.max * 8);

//    workers.length = totalCPUs - 1;
    workers.length = 4;

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
        // all need be initted before we start them
    {
        mixin(zoneMixin("threadStartup"));
        foreach(i; 0 .. workers.length)
        {
            (cast()workers[i].workerThread).start();
        }
    }

    string fName = "a";
    string[] result;
    addTask(Task(&loadFiles, cast(shared void*)&fName, false, cast(shared void*)&result));
    printf("tasksInQueue zero: %d\n", queues[0].tasksInQueue());
    Thread.sleep(msecs(60));
    // you have half a second.
    foreach(i; 0 .. workers.length)
    {
        queues[i].enqueueTermination();
    }
    // task should now be in queue zero
    printf("tasksInQueue zero: %d\n", queues[0].tasksInQueue());

    {
        mixin(zoneMixin("Worker-Time"));
        foreach(i; 0 .. workers.length)
        {
            (cast()workers[i].workerThread).join();
        }
    }
}
