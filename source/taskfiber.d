module fluffy.taskfiber;

import fluffy.taskgroup;

import core.stdc.stdio;

import core.atomic;

import core.thread.myFiber;
import fluffy.ticket;

alias task_function_t = void function (Task*);

class TaskFiber : Fiber
{
    /*tls*/ static bool initLoop;
    Task currentTask;
    int idx;
    bool hasTask;

    this(int idx = int.max)
    {
        assert(idx != int.max);
        super(&doTask, ushort.max * 8);
        // use large stack of ushort.max * 8
        // otherwise we can crash in the parser or deeper semantic
        this.idx = idx;
    }

    void doTask()
    {
        if (hasTask)
        {
            assert(state() != State.TERM, "Attempting to start a finished task");
            currentTask.fn(&currentTask);
            {
                // string s = stateToString(state());
                // printf("Task state after calling fn: %s\n", s.ptr);
            }
        }
        else
            assert(initLoop, "hasTask can only be false when doTask is called during initalisation");

    }

    void assignTask(Task* task)
    {
        assert(!hasTask);
        this.currentTask = *task;
        hasTask = true;
    }

    static string stateToString(typeof(new Fiber((){}).state()) state)
    {
        final switch (state)
        {
            case state.TERM:
               return "Done";
            case state.HOLD:
               return "Suspended";
            case state.EXEC:
               return "Running";
        }
    }

    bool hasCompleted()
    {
        return (state() == State.TERM);
    }
}

struct FiberPool
{
@("tracy"):
    TaskFiber[freeBitfield.sizeof * 8] fibers = null;
    void* fiberPoolStorage = null;

    uint scheduled;
    uint completed;
    uint inUse;

    uint freeBitfield = (~0);
    
    static immutable INVALID_FIBER_IDX = 0;
    
    uint nextFree()
    {
        pragma(inline, true);
        import core.bitop : bsf;
        return freeBitfield ? bsf(freeBitfield) + 1 : INVALID_FIBER_IDX;
    }
    
    uint n_free()
    {
        pragma(inline, true);
        import core.bitop : popcnt;
        return popcnt(freeBitfield);
    }
    
    uint n_used()
    {
        pragma(inline, true);
        import core.bitop : popcnt;
        return cast(uint) (fibers.length - popcnt(freeBitfield));
    }
    
    bool isFull()
    {
        pragma(inline, true);
        return !n_free();
    }
    
    void initFiberPool()
    {
        import core.stdc.stdlib;
        version (none)
        {
            static immutable aligned_size = align16(__traits(classInstanceSize, TaskFiber));
            this.fiberPoolStorage = malloc(aligned_size * fibers.length);
            pool.fibers = (cast(TaskFiber*)pool.fiberPoolStorage)[0 .. fibers.length];
        }
        TaskFiber.initLoop = true;
        foreach(int idx, ref f;this.fibers)
        {
            version (none)
            {
                f = (cast(TaskFiber)(this.fiberPoolStorage + (aligned_size * idx)));
                f.__ctor(idx);
            }
            f = new TaskFiber(idx);
        }
        TaskFiber.initLoop = false;
    }
    
    bool isInitialized()
    {
        pragma(inline, true);
        return fibers[0] !is null;
    }
    
    void free(TaskFiber fiber)
    {
        assert(!fiber.hasTask);
        completed++;
        inUse--;
        const fiberIdx = fiber.idx;
        assert(fiberIdx < fibers.length);
        freeBitfield |= (1 << fiberIdx);
    }
    
    TaskFiber getNextFree() return
    {
        if (const fiberIdx = nextFree())
        {
            assert(fiberIdx != INVALID_FIBER_IDX);
            scheduled++;
            inUse++;
            freeBitfield &= ~(1 << (fiberIdx - 1));
            return fibers[fiberIdx - 1];
        }
        assert(0);
        //return null;
    }
}

struct Task
{
    task_function_t fn;
    shared (void*) taskData;
    // shared (void*) taskResult;
    shared (TicketCounter)* syncLock;

    this(task_function_t myFn, shared void *myTaskData = null, shared (TicketCounter*) myLock = null)
    {
        import core.atomic;
        this.fn = myFn;
        this.taskData = myTaskData;
        this.syncLock = myLock;
        this.taskId = atomicOp!"+="(runningId, 1);
    }

    align(16) shared TicketCounter taskLock;

    shared Task*[] children;
    shared size_t n_children_completed;

    uint queueID;

    shared (TaskFiber)* currentFiber;
    shared bool hasCompleted_ = false;
    shared bool hasFiber = false;
    shared bool fiberIsExecuting = false;

    Ticket creation_ticket;
    uint completion_attempts;

    bool hasCompleted(string file = __FILE__, int line = __LINE__) shared
    {
        // printf("[%s:+%d]asking has Completed \n", file.ptr, line);
        return (atomicLoad(hasCompleted_) || (atomicLoad(hasFiber) && !fiberIsExecuting && (cast(TaskFiber*)currentFiber).hasCompleted()));
    }
    //debug (task)
    //{
        OriginInformation originInfo;
    //}

    TaskGroup* taskgroup;

    uint taskId;
    uint schedulerId;
    shared static uint runningId;
    shared static uint runningSchedulerId;
}

size_t align16(const size_t n) pure
{
    pragma(inline, true);
    return ((n + 15) & ~15);
}

struct OriginInformation
{
    string filename;
    uint line;
    shared Task* originator;
}
