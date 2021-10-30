module fluffy.taskgroup;
import fluffy.taskfiber;
import fluffy.app;
import core.atomic;
import core.stdc.stdio;

struct TaskGroup
{
    WorkersQueuesAndWatcher* workers;
    string name;
    TaskGroup* parent;
    shared Alloc taskGroupAllocator;

    this(WorkersQueuesAndWatcher* myWorkers, string myName, TaskGroup *myParent = null)
    {
        workers = myWorkers;
        name = myName;
        parent = myParent;
        taskGroupAllocator = cast(shared)Alloc(ushort.max * 32);
    }

    ~this()
    {
        awaitCompletionOfAllTasks();
        //TODO free allocations!
    }

    Task*[] tasks;
    Task* addTask(alias F, ArgTypeTuple ...)(ArgTypeTuple args) return
    {
        struct ArgumentHolder
        {
           ArgTypeTuple args;
        }
        auto task = cast(Task*) taskGroupAllocator.alloc(Task.sizeof);
        auto holder = cast(ArgumentHolder*) taskGroupAllocator.alloc(ArgumentHolder.sizeof);
        *holder = ArgumentHolder(args);

        *task = Task((Task* taskP)
        {
            auto argsP = cast(ArgumentHolder*) taskP.taskData;
            F(argsP.args);
            atomicFence!(MemoryOrder.seq);
            atomicStore(taskP.hasCompleted_, true);
        }, cast (shared void*)holder);
        task.taskgroup = &this;
        tasks ~= task;
        return task;
    }
    /// set shouldCompleteImmediately to denote that.
    /// we will abort if it does not
    void awaitCompletionOfAllTasks(bool shouldCompleteImmediately = false)
    {
        foreach(t;tasks)
        {
            // do not attempt to run a completed task again.
            if (!atomicLoad!(MemoryOrder.raw)(t.hasCompleted_))
                .addTask(t);
        }

        foreach(t;tasks)
        {
            while(!atomicLoad!(MemoryOrder.raw)(t.hasCompleted_)) {  }
        }

    }
}
