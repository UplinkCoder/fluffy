module fluffy.taskqueue;
import fluffy.taskfiber;
import fluffy.ticket;
import core.atomic;

version (tracy)
{
    import tracy;
}
else
{
    string zoneMixin(string zone)
    {
        return "";
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
                        (cast(Task[])queue[writeP .. writeP + n]) = task[0 .. n];
                    }
                    else
                    {
                        int overhang = cast(int)((writeP + n) - queue.length);
                        // this is how much we cannot fit
                        // therefore n - overhang is how much we can fit at the end
                        int first_part = n - overhang;


                        (cast(Task[])queue[writeP .. $]) = task[0 .. first_part];
                        (cast(Task[])queue[0 .. n - first_part]) = task[first_part .. n];
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

