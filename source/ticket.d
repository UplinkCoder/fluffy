module fluffy.ticket;

public import core.atomic;

import core.stdc.stdio;

import fluffy.intel_inspector;
import core.sys.posix.pthread;

struct Ticket
{
    uint ticket;
}

/// Ticket Lock ordered syncronisation mechanism
struct TicketCounter
{
    version (pmutex)
    {
    bool mutex_inited = false;
    pthread_mutex_t mutex;
    }
    static string unlockMixin(string lockName)
    {
        return  "atomicFence!(MemoryOrder.seq)();\n"
            ~   lockName ~ ".releaseTicket(ticket);";
    }

    static string lockMixin(string lockName)
    {
        return  "const ticket = " ~ lockName ~ ".drawTicket();"
            ~   "while(!" ~lockName ~ ".servingMe(ticket)) {}\n"
            ~   "atomicFence!(MemoryOrder.seq)();\n";
    }

@nogc: nothrow:
    version (no_sync)
    {
        uint nextTicket = 0;
        uint currentlyServing;
    }
    else
    {
        shared align(16) uint nextTicket = 0;
        shared align(16) uint currentlyServing;
    }

    uint apporxQueueLength() shared
    {
        return currentlyServing - nextTicket;
    }

    Ticket drawTicket(string func = __FUNCTION__, string file = __FILE__, int line = __LINE__) shared pure
    {
        pragma(inline, true);
        version (no_sync)
        {
            auto newTicket = nextTicket + 1;
            nextTicket = newTicket;
            return Ticket(nextTicket - 1);
        }
        else
        {
            version (pmutex)
            {
            if (cas(&mutex_inited, false, true))
            {
                {
                    pthread_mutex_init(cast(pthread_mutex_t*)&mutex, null);
                }
            }
            }
            __itt_sync_prepare(cast(void*) &this);
            return Ticket(atomicOp!"+="(nextTicket, 1) - 1);
        }
    }

    void releaseTicket(Ticket ticket) shared pure
    {
        pragma(inline, true);
        version (no_sync)
        {
            auto currentlyServing_ = currentlyServing + 1;
            currentlyServing = currentlyServing_;
        }
        else
        {
            __itt_sync_releasing(cast(void*) &this);
            assert(currentlyServing == ticket.ticket);
            atomicOp!"+="(currentlyServing, 1);
            version (pmutex)
            {
                pthread_mutex_unlock(&mutex);
            }
        }
    }

    bool servingMe(Ticket ticket, string func = __FUNCTION__, string file = __FILE__, int line = __LINE__) shared pure
    {
        pragma(inline, true);
        version (no_sync)
        {
            return currentlyServing == ticket.ticket;
        }
        else
        {
            version (pmutex)
            {
                auto result = pthread_mutex_trylock(&mutex) == 0;
            }
            else
            {
                auto result = atomicLoad(currentlyServing) == ticket.ticket;
            }
            if (result)
            {
                __itt_sync_acquired(cast(void*) &this);
            }
            return result;
        }
    }

    void redrawTicket(ref Ticket ticket, string func = __FUNCTION__, string file = __FILE__, int line = __LINE__) shared
    {
        pragma(inline, true);
        releaseTicket(ticket);
        ticket = drawTicket(func, file, line);
    }

}


string uniqueName(string prefix, string file = __FILE__, uint line = __LINE__) pure
{
    import core.internal.string : unsignedToTempString;
    return prefix ~ "_" ~ file ~ "_" ~ unsignedToTempString(line) ~ "_";
}

struct TestSetLock
{
    align(16) shared bool unlocked = true;

    bool tryLock() shared nothrow pure
    {
        pragma(inline, true);
        auto result = cas(&unlocked, true, false);

        if (result)
            __itt_sync_acquired(cast(void*) &this);
        return result;
    }

    void unlock() shared nothrow pure
    {
        pragma(inline, true);
        __itt_sync_releasing(cast(void*) &this);
        atomicStore(unlocked, true);
    }

    static string unlockMixin(string lockName)
    {
        return  "import core.atomic;"
            ~   "atomicFence!(MemoryOrder.seq)();\n"
            ~   lockName ~ ".unlock();";
    }

    static string lockMixin(string lockName)
    {
        return  "import core.atomic;"
            ~   "while(!" ~lockName ~ ".tryLock()) {}\n"
            ~   "atomicFence!(MemoryOrder.seq)();\n";
    }
}



version (LDC)
    private import ldc.intrinsics : llvm_atomic_cmp_xchg, llvm_atomic_rmw_add;
else
    private import core.atomic : cas, atomicOp;

private pragma(inline, false) void fatal(string errorDescription) @nogc nothrow pure @trusted
{
    // callToYourNotifierHere(errorDescription);
    assert(0, errorDescription);
}

struct NBRWLock
{
    // the lock is not reentrant wrt writing but reentrant read acquisition is fine
    // bit 31, the sign bit, indicates a writer is active
    // bits 0..30 keep a count of the number of active readers (ok to be inexact transiently)
    // the lock becomes racy once the active reader count hits 2^31 - 1
    align(4) private shared int _rwctr = 0;
    @disable this(this); // no valid use case is known for this ctor so we disable it

    pragma(inline, true) bool tryReadLock() @nogc nothrow pure @safe shared
    {
        if (const acquired = rmwAdd(1) > 0)
            return true;
        rmwAdd(-1); // a writer must have been active, back out our increment of the lsbs
        return false;
    }

    pragma(inline, true) void releaseReadLock() @nogc nothrow pure @safe shared
    {
        rmwAdd(-1) >= 0 || fatal("read lock underflow on release");
    }

    pragma(inline, true) bool tryWriteLock() @nogc nothrow pure @safe shared
    {
        version (LDC)
            return llvm_atomic_cmp_xchg(&_rwctr, 0, int.min).exchanged;
        else
            return cas(&_rwctr, 0, int.min);
    }

    pragma(inline, true) void releaseWriteLock() @nogc nothrow pure @safe shared
    {
        rmwAdd(int.min) >= 0 || fatal("write lock confusion on release");
    }

    private pragma(inline, true) int rmwAdd(int value) @nogc nothrow pure @safe shared
    {
        version (LDC)
            return value + llvm_atomic_rmw_add(&_rwctr, value); // returns pre op value so adjust
        else
            return atomicOp!"+="(_rwctr, value); // returns post op value so return it directly
    }


    static string writeLockMixin(string lockName)
    {
        return  "import core.atomic;"
            ~   "while(!" ~lockName ~ ".tryWriteLock()) {}\n"
            ~   "atomicFence!(MemoryOrder.seq)();\n";
    }

    static string readLockMixin(string lockName)
    {
        return  "import core.atomic;"
            ~   "while(!" ~lockName ~ ".tryReadLock()) {}\n"
            ~   "atomicFence!(MemoryOrder.seq)();\n";
    }

    static string writeUnlockMixin(string lockName)
    {
        return  "import core.atomic;"
            ~   "atomicFence!(MemoryOrder.seq)();\n"
            ~   lockName ~ ".releaseWriteLock();";
    }

    static string readUnlockMixin(string lockName)
    {
        return  "import core.atomic;"
            ~   "atomicFence!(MemoryOrder.seq)();\n"
            ~   lockName ~ ".releaseReadLock();";
    }

}

