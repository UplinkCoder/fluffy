module fluffy.intel_inspector;

import core.sys.posix.dlfcn;
import core.stdc.stdio;
import core.stdc.stdlib;
import core.stdc.string;

extern (C) @nogc pure nothrow __gshared {
    void dummy (void*) {};

    static const void function  (void *addr, const char *objtype, const char *objname, int attribute) __itt_sync_create
        = (void *addr, const char *objtype, const char *objname, int attribute){};
    /**
    * @brief Quit spin loop without acquiring spin object
    */
    static const void function (void* addr) __itt_sync_cancel = &dummy;
    /**
    * @brief Successful spin loop completion (sync object acquired)
    */
    static const void function (void* addr) __itt_sync_acquired = &dummy;
    /**
    * @brief Start sync object releasing code. Is called before the lock release call.
    */
    static const void function (void* addr) __itt_sync_releasing = &dummy;

    /**
    * @brief Start sync object spinloc k code.
    */
    static const void function (void* addr) __itt_sync_prepare = &dummy;
}
version (MARS) {}
else
{
shared static this()
{

    auto insp_dir_c = getenv("INSPECTOR_2021_DIR");
    auto insp_dir = insp_dir_c ? cast(string) insp_dir_c[0 .. strlen(insp_dir_c)] : null;
    if (insp_dir)
    {
        char[1024] pathbuf;
        sprintf(pathbuf.ptr, "%s/lib64/runtime/libittnotify.so", insp_dir_c);
        auto lib = dlopen(pathbuf.ptr, RTLD_LAZY);
        if (lib)
        {
            cast()__itt_sync_cancel = cast(typeof(__itt_sync_cancel))dlsym(lib, "__itt_sync_cancel");
            cast()__itt_sync_create = cast(typeof(__itt_sync_create))dlsym(lib, "__itt_sync_create");
            cast()__itt_sync_acquired = cast(typeof(__itt_sync_acquired))dlsym(lib, "__itt_sync_acquired");
            cast()__itt_sync_releasing = cast(typeof(__itt_sync_releasing))dlsym(lib, "__itt_sync_releasing");
            cast()__itt_sync_prepare = cast(typeof(__itt_sync_prepare))dlsym(lib, "__itt_sync_prepare");

            return ;
        }
    }

    fprintf(stderr, "intel inspector library functions could not be loaded\n");
}
}
