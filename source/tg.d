/+
TaskGroup tg;

tg.addTask(Task((Task* task)
{
    auto s = *cast (string*) task.arg;
    
}), alloc.String("Hello World"), 
    cast(void*)&hash, &hashSyncLock);
+/
