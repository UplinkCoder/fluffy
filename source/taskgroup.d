module fluffy.taskgroup;
import fluffy.taskfiber;
import fluffy.app;
import fluffy.tracy;
import core.atomic;
import core.stdc.stdio;


static immutable task_function_t TaskCompletionBarrierDg =
(Task*) { string x = "Task Completion Barrier"; };
// needs a unqiue function body to not be merged

struct BarrierArgs
{
    uint barrierBegin;
    uint barrierEnd;
}


TaskGroup* allocateInParent(string groupName)
{
    TaskGroup* result;
    if (auto f =  TaskFiber.getThis())
    {
        auto tf = *cast(TaskFiber*) &f;
        assert(tf.currentTask.taskgroup);
        result = tf.currentTask.taskgroup.allocateChildGroup(groupName);
    }
    
    return result;
}

struct TaskGroup
{
    WorkersQueuesAndWatcher* workers;
    string name;
    TaskGroup* parent;
    shared Alloc* taskGroupAllocator;
    size_t lastCompletionBarrier;

    TaskGroup*[] children;

    this(WorkersQueuesAndWatcher* myWorkers, string myName, shared Alloc* alloc)
    {
        workers = myWorkers;
        name = myName;
        parent = null;
        taskGroupAllocator = alloc;
    }
    
    void awaitChild(TaskGroup* child)
    {
        static char* printFn(Task* task)
        {
            struct ArgumentHolder
            {
                TaskGroup* c;
            }
            import core.stdc.stdlib;
            auto args = cast(ArgumentHolder*)task.taskData;
            char[4096] buf;
            int sz = sprintf(buf.ptr, "awaiting child group: %s [%d tasks]", args.c.name.ptr, cast(int)args.c.tasks.length);
            foreach(ctask;args.c.tasks)
            {
                printf("%s\n", taskGraph(ctask).ptr);
            }
            char* result = cast(char*)malloc(sz + 1);
            assert(result);
            result[0 .. sz] = buf[0 .. sz];
            result[sz] = '\0';
            return result;
        }
        
        this.addTaskWithPrint!((TaskGroup* c)
            {
                foreach(task;c.tasks)
                {
                }
                c.awaitCompletionOfAllTasks();
            })(&printFn, child);
        // print function
    }
    
    TaskGroup* allocateChildGroup(string name)
    {
        TaskGroup* result = cast(TaskGroup*)taskGroupAllocator.alloc(TaskGroup.sizeof);
        result.workers = workers;
        result.parent = &this;
        result.name = name;
        result.taskGroupAllocator = taskGroupAllocator;
        
        return result;
    }
    
    ~this()
    {
        awaitCompletionOfAllTasks();
        //TODO free allocations!
    }
    
    Task*[] tasks;
    import std.traits;
    
    static template AliasSeq(Seq...)
    {
        alias AliasSeq = Seq;
    }
    
    static template Comma(alias Arg)
    {
        alias Comma = AliasSeq!(Arg, ", ");
    }
    
    
    Task* addTask(alias F, ArgTypeTuple ...)(ArgTypeTuple args) return
    {
        pragma(msg, "please consider adding a custom print function to ", __traits(getLocation, F)[0], ":", __traits(getLocation, F)[1]);
        struct ArgumentHolder
        {
            ArgTypeTuple args;
        }
        
        static char* printFn (Task* task) {
            import core.stdc.stdlib;
            char* result;
            char[4096] buffer;
            char* ptr = &buffer[0];

            version (MARS)
            {
                import dmd.ast_node;
                import dmd.dimport;
                import dmd.asttypename;
            }
            foreach(i, ArgType;ArgTypeTuple)
            {
                pragma(msg, ArgType);
                version (MARS)
                {
                    static if (is(ArgType : Import))
                    {
                        ptr += sprintf(ptr, "import %s, ",
                            (*cast(ArgType*)(task.taskData + ArgumentHolder.tupleof.offsetof[i])).mod.toChars());
                    }
                    else static if (is(ArgType : ASTNode))
                    {
                        ptr += sprintf(ptr, "[%s] %s, ",
                            (*cast(ArgType*)(task.taskData + ArgumentHolder.tupleof.offsetof[i])).astTypeName.ptr,
                            (*cast(ArgType*)(task.taskData + ArgumentHolder.tupleof.offsetof[i])).toChars()
                            );
                    }
                    else static if (is(typeof(ArgType.init.toChars()) : const(char)*))
                    {
                        ptr += sprintf(ptr, "%s, ",
                            (*cast(ArgType*)(task.taskData + ArgumentHolder.tupleof.offsetof[i])).toChars());
                    }
                }
/+
                import std.format;
                import std.string;
                ptr += sprintf(ptr, "%s, ", format("%s", (*cast(ArgType*)(task.taskData + ArgumentHolder.tupleof.offsetof[i]))).replace("\"", "\\\"").toStringz);
+/                
            }
            auto len = ptr - &buffer[0];
            if (len)
            {
                result = cast(char*)malloc(len - 1);
                result[0 .. len - 2] = buffer[0 .. len - 2];
                result[len-2] = '\0';
            }
            return result;
        }
        
        return addTaskWithPrint!(F) (&printFn, args);
    }
    
    Task* addTaskWithPrint(alias F, ArgTypeTuple ...)(char* function (Task*) printFunction, ArgTypeTuple args) return
    {
        // import std.stdio; writeln ("Adding task: ", __traits(identifier, F), " +",__traits(getLocation, F)[1], " ", __traits(getLocation, F)[0],"  -- (", staticMap!(Comma, args), ")");
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
            }, printFunction, cast(shared void*)holder);
        
        if (auto tf = cast(TaskFiber) TaskFiber.getThis())
        {
            task.parent = tf.currentTask;
            version (MARS)
            {
                task.task_local_state = task.parent.task_local_state;
            }
            const ticket = task.parent.childLock.drawTicket();
            
            while(!task.parent.childLock.servingMe(ticket)) {}
            
            atomicFence!(MemoryOrder.seq);
            
            task.parent.children ~= task;
            
            atomicFence!(MemoryOrder.seq);
            task.parent.childLock.releaseTicket(ticket);
        }
        
        task.taskgroup = &this;
        tasks ~= task;
        // printf("taskAdded: %p\n", task);
        return task;
    }
    /// set shouldCompleteImmediately to denote that.
    /// we will abort if it does not
    void awaitCompletionOfAllTasks(bool shouldCompleteImmediately = false)
    {
        foreach(t;tasks[lastCompletionBarrier .. $])
        {
            // do not attempt to run a completed task again.
            if (!atomicLoad!(MemoryOrder.raw)(t.hasCompleted_))
                .addTask(t);
        }

        Task* barrierTask = cast(Task*) taskGroupAllocator.alloc(Task.sizeof);
        auto barrierArgsP = cast(BarrierArgs*) taskGroupAllocator.alloc(uint.sizeof * 2);
        BarrierArgs barrierArgs =
        { barrierBegin : cast(uint)lastCompletionBarrier,
        barrierEnd   :  cast(uint)tasks.length };
        (*barrierArgsP) = barrierArgs;
        barrierTask.taskData = cast(shared void*) barrierArgsP;
        barrierTask.fn = TaskCompletionBarrierDg;
        barrierTask.printFunction = (Task* task) {
            char[4096] buffer;
            char* ptr = &buffer[0];
        
            ptr += sprintf(ptr, "Task Completion Barrier for : {\n");
            auto barrier = *cast(BarrierArgs*)task.taskData;
            auto tg = task.taskgroup;
            foreach(bTask;tg.tasks[barrier.barrierBegin .. barrier.barrierEnd])
            {
                ptr += sprintf(ptr, "    %s\n", bTask.printFunction(bTask   ));
            }
            ptr += sprintf(ptr, "}");

            return buffer.ptr[0 .. ptr - buffer.ptr].dup.ptr;
        };
        barrierTask.taskgroup = &this;
        tasks ~= barrierTask;
        
        // add new tasks if we are done yield to work on them
        if (auto tf = TaskFiber.getThis())
        {
            TaskFiber task_fiber = *cast(TaskFiber*)&tf;
            task_fiber.currentTask.children ~= barrierTask;
            task_fiber.suspend();
        }
        {
            mixin(zoneMixin("await completion of all tasks"));
            
            micro_sleep(1);
        }
        // lets give the tasks a little time before we yield in a loop
        uint loopCounter;
    LloopBegin:
        loopCounter++;
        bool uncompleted_tasks = false;
        foreach(t;tasks[lastCompletionBarrier .. $-1])
        {
            while(!atomicLoad!(MemoryOrder.raw)(t.hasCompleted_))
            {
                uncompleted_tasks = true;
                if (loopCounter > 1000)
                {
                    // printf("task doesn't seem to complete: %s\n", t.taskgroup.name.ptr);
                }
                
                if (auto tf = TaskFiber.getThis())
                {
                    TaskFiber task_fiber = *cast(TaskFiber*)&tf;
                    task_fiber.suspend();
                }
                else
                {
                    micro_sleep(1);
                }
                break;
            }
            // here the task has completed
            version(MARS)
            {
                import dmd.globals;
                if (task_local.gag)
                {
                    /+
                    task_local.gaggedErrors += t.task_local_state.gaggedErrors;
                    task_local.gaggedWarnings += t.task_local_state.gaggedWarnings;
+/
                }
            }
            
        }
        if (uncompleted_tasks)
            goto LloopBegin;
        
        lastCompletionBarrier = tasks.length;
    }
}

uint barrierIndex(Task* task)
{
    if (!task.taskgroup)
        return false;
    
    uint lastBarrierBegin;
    uint lastBarrierEnd;
    uint lastBarrierIndex;
    foreach_reverse(i, t; task.taskgroup.tasks)
    {
        if (t is task)
        {
            if (i >= lastBarrierBegin && i < lastBarrierEnd)
            {
                return lastBarrierIndex + 1;
            }
            else
            {
                return 0;
            }
        }
        else if (t.fn is TaskCompletionBarrierDg)
        {
            auto ba = *cast(BarrierArgs*) t.taskData;
            lastBarrierBegin = ba.barrierBegin;
            lastBarrierEnd = ba.barrierEnd;
            lastBarrierIndex = cast(uint)i;
        }
    }
    assert(0);
}

string taskGraph(Task* task, Task* parent = null, int indent = 0)
{
    static char[] formatTask(Task* t)
    {
        import core.stdc.stdlib;
        enum formatBufferLength = 4096 * 128;
        static char[] formatBuffer = null;
        if (!formatBuffer.ptr)
        {
            formatBuffer =
                cast(char[])malloc(formatBufferLength)[0  .. formatBufferLength];
        }
        
        string result;
        
        if (!t)
            return cast(char[])"\"null, (TheRoot)\"";
        

        

        
        auto tg = t.taskgroup;
        TaskGroup*[255] taskGroupStack;
        uint taskStackDepth;
        char * endP = formatBuffer.ptr;

        if (auto bIdx = barrierIndex(t))
        {
            auto barrierTask = t.taskgroup.tasks[bIdx - 1];
            assert(barrierTask.fn is TaskCompletionBarrierDg);
            import std.format;
            assert(t.fn !is TaskCompletionBarrierDg);
            
            endP += sprintf(endP, "\"[%p] TaskBarrier \" -> ", barrierTask);
            endP += sprintf(endP, "\"[%p] Task('%s')\"", t, t.printFunction(t));
        }
        else
        {

            for(auto p = tg; p.parent; p = p.parent)
            {
                taskGroupStack[taskStackDepth++] = p;
            }
            uint len = 0;

            /+
            foreach_reverse(tg_;taskGroupStack[0 .. taskStackDepth])
            {
                endP += sprintf(endP, "\"%p TG(%s) \" -> ", tg_, tg_ ? tg_.name.ptr : null);
            }
    +/
            endP += sprintf(endP, "\"[%p] TaskGroup(%s) \" -> ", tg, tg ? tg.name.ptr : null);
            if (t.fn is TaskCompletionBarrierDg)
            {
                endP += sprintf(endP, "\"[%p] TaskBarrier \"", t);
            }
            else
            {
                endP += sprintf(endP, "\"[%p] Task('%s')\"", t, t.printFunction(t));
            }
        }
        return formatBuffer[0 .. endP - formatBuffer.ptr].dup;
    }
    
    string result;
    if (parent is null)
    {
        result ~= "digraph \"" ~ task.taskgroup.name ~ "\" {\n";
    }
    
    foreach(_; 0 .. indent++)
    {
        result ~= "\t";
    }
    if (!parent || parent.fn !is TaskCompletionBarrierDg)
    {
        result ~= formatTask(parent) ~ " -> " ~ formatTask(task) ~ "\n";
    }
    
    foreach(ref c;task.children)
    {
        if (c.fn is TaskCompletionBarrierDg)
            continue;
        
        result ~= taskGraph(cast(Task*)c, task, indent);
        //result ~= formatPtr(task) ~ " > " ~ formatPtr(c);
    }
    
    if (parent is null)
    {
        result ~= "\n}";
    }
    
    return result;
}
