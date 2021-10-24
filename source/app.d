import core.memory : GC;
import fluffy.taskfiber;
import fluffy.ticket;
import core.thread;
import core.atomic;
import core.stdc.stdio;

static  immutable shared (void*) delegate (shared void*) terminationDg = 
    (shared void*) { return cast(shared void*)null; };

struct TaskQueue
{
    shared TicketCounter queueLock;

    Task[1024] queue;
    /// a read pointer of -1 signifies the queue is empty

    align(16) shared int readPointer; // head
    align(16) shared int writePointer; // tail

    int tasksInQueue() shared
    {
        const readP = atomicLoad(readPointer) & (queue.length - 1);
        const writeP = atomicLoad(writePointer) & (queue.length - 1);
        return cast(int)(writeP - readP);
    }

    bool enqueueTermination() shared
    {
        auto terminationTask = Task(terminationDg);
        return push(&terminationTask);
    }

    bool push(Task* task) shared
    {
        printf("pushing\n");
        auto ticket = queueLock.drawTicket();
        while(!queueLock.servingMe(ticket)) {}
        // we've got the lock
        {
            scope (exit) queueLock.releaseTicket(ticket);
            const readP = atomicLoad(readPointer) & (queue.length - 1);
            const writeP = atomicLoad(writePointer) & (queue.length - 1);
            // update readP and writeP
            if (readP == writeP + 1)
            {
                assert(tasksInQueue() == queue.length);
                return false;
            }

            cast()queue[writeP] = *task;
            atomicOp!"+="(writePointer, 1);
        }

        return true;
    }

    bool pull(Task* task) shared
    {
        // printf("try pulling\n");
        // as an optimisation we check for an empty queue first
        {
            const readP = atomicLoad!(MemoryOrder.raw)(readPointer) & (queue.length - 1);
            const writeP = atomicLoad!(MemoryOrder.raw)(writePointer) & (queue.length - 1);
            // printf("before pull -- readP: %d, writeP: %d\n", readP, writeP);
            // update readP and writeP
            if (readP == writeP)
            {
                assert(tasksInQueue() == 0);
                return false;
            }
        }
        auto ticket = queueLock.drawTicket();
        while(!queueLock.servingMe(ticket)) {}

        {
            scope (exit) queueLock.releaseTicket(ticket);
            const readP = atomicLoad(readPointer) & (queue.length - 1);
            const writeP = atomicLoad(writePointer) & (queue.length - 1);
            // printf("before pull -- readP: %d, writeP: %d\n", readP, writeP);
            // update readP and writeP
            if (readP == writeP)
            {
                assert(tasksInQueue() == 0);
                return false;
            }

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

struct Worker
{
    Thread workerThread;
shared:
    TaskQueue queue;
}

shared Worker[] workers;

void workerFunction () {
    static shared int workerCounter;
    int workerIndex = atomicOp!"+="(workerCounter, 1) - 1;
    printf("Startnig: %d\n", workerIndex);
    shared(TaskQueue)* myQueue = &workers[workerIndex].queue;
    bool terminate = false;
    int myCounter = 0;
    Task task;
    while(!terminate)
    {
        if ((myQueue.pull(&task)))
        {
            if (task.fn is terminationDg)
            {
                terminate = true;
                printf("[%d] recieved termination signal\n", workerIndex);
            }
        }
        import core.stdc.stdio;
        if (myCounter++ == 12 || terminate)
        {
            int target = cast(int) (workerIndex ? workerIndex - 1 : workers.length - 1);
            printf("[%d] sending termination signal to [%d]\n", workerIndex, target);
            while (!workers[target].workerThread) {} // wait for target to be born

            workers[target].queue.enqueueTermination(); // kill target
        }

    }
}
void main()
{
    import std.parallelism : totalCPUs;

    workers.length = totalCPUs;

    import core.stdc.stdio;

    printf("Found %d cores\n", totalCPUs);
    foreach(i; 0 .. totalCPUs())
    {
        workers[i] = cast(shared) Worker(new Thread(&workerFunction));
    }
    // all need be initted before we start them
    foreach(i; 0 .. totalCPUs())
    {
        (cast()workers[i].workerThread).start();
    }
    foreach(i; 0 .. totalCPUs())
    {
        (cast()workers[i].workerThread).join();
    }
}
