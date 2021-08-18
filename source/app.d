import core.memory : GC;
import fluffy.taskfiber;

extern (C) Task[]* tasks; 
void main()
{
  GC.disable();

  shared(void*) fib_fn (shared void* data) {
    int n = cast(int)data;
    import core.stdc.stdio;
    printf("going to compute fib of %d\n", n);
    scope(exit)
        printf("task for fib of %d is done\n", n);
    if (n == 0) return cast(void*)0;
    if (n == 1) return cast(void*)1;
    else
    {
      int result; 
      // auto task = fib_tg.addTask(&fib_fn, cast(shared void*)(n - 1), true);
      result += cast(int)fib_fn(cast(shared void*)(n - 2));
      // fib_tg.awaitCompletionOfAllTasks();
      // result += (cast(int)task.result); 
      return cast(shared void*)result;
    }
  }

//  asm { int 3; }
}
