module fluffy.ticket;

import core.atomic;

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
    bool mutex_inited = false;
    pthread_mutex_t mutex;
@nogc: nothrow:
    shared align(16) uint nextTicket = 0;
    shared align(16) uint currentlyServing;


    Loc lastAquiredLoc;
    struct Loc
    {
        string func;
        string file;
        int line;
    }
    int n_waiters;

    Ticket drawTicket(string func = __FUNCTION__, string file = __FILE__, int line = __LINE__) shared
    {
        if (cas(&mutex_inited, false, true))
        {
            pthread_mutex_init(cast(pthread_mutex_t*)&mutex, null);
        }
        pragma(inline, true);
        __itt_sync_prepare(cast(void*) &this);
        return Ticket(atomicOp!"+="(nextTicket, 1) - 1);
    }

    void releaseTicket(Ticket ticket) shared
    {
        pragma(inline, true);

        lastAquiredLoc = Loc("free","free", 0);
        __itt_sync_releasing(cast(void*) &this);
        atomicOp!"+="(currentlyServing, 1);
        pthread_mutex_unlock(&mutex);
    }

    bool servingMe(Ticket ticket, string func = __FUNCTION__, string file = __FILE__, int line = __LINE__) shared
    {
        pragma(inline, true);

        auto result = pthread_mutex_trylock(&mutex) == 0;
/+
        auto result = atomicLoad(currentlyServing) == ticket.ticket;
        if (result)
        {
            __itt_sync_acquired(cast(void*) &this);
            lastAquiredLoc = Loc(func, file, line);
        }
+/       
        return result;
    }

    void redrawTicket(ref Ticket ticket, string func = __FUNCTION__, string file = __FILE__, int line = __LINE__) shared
    {
        pragma(inline, true);
        releaseTicket(ticket);
        ticket = drawTicket(func, file, line);
    }
}


struct TestSetLock
{
    align(16) shared bool unlocked = true;
    bool tryLock() shared nothrow
    {
        pragma(inline, true);
        auto result = cas(&unlocked, true, false);

        if (result)
            __itt_sync_acquired(cast(void*) &this);
        return result;
    }
    void unlock() shared nothrow
    {
        pragma(inline, true);
        __itt_sync_releasing(cast(void*) &this);
        atomicStore(unlocked, true);
    }
}
