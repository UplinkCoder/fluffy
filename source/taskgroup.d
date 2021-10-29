module fluffy.taskgroup;
import fluffy.taskfiber;

struct TaskGroup
{
    string name;
    TaskGroup* parent;

    Task[] tasks;
    Task* addTask(alias F, ArgTypeTuple ...)(ArgTypeTuple args) return
    {
        struct ArgumentHolder
        {
           ArgTypeTuple args;
        }
        auto holder = ArgumentHolder(args);
        Task task = Task((Task* task)
        {
            auto argsP = cast(ArgumentHolder*) task.taskData;
            F(argsP.args);
        }, cast (shared void*)&holder);
        task.taskgroup = &this;
        tasks ~= task;
        return &tasks[$-1];
    }
}
