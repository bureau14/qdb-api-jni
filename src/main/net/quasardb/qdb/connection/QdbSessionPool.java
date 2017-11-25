package net.quasardb.qdb;

import java.util.concurrent.TimeUnit;
import stormpot.*;
import net.quasardb.qdb.*;

/**
 * Provides an object pool which allows for sharing and re-use of quasardb
 * connections over a period of time.
 */
class QdbSessionPool {

    /**
     * Keeps track of all the currently allocated sessions.
     */
    private Pool<PoolableSession> pool;

    /**
     * A QdbSession that provides the interface necessary for connection pool
     * management. The user of this class must be careful to call release()
     * after they are done with the class.
     */
    private static class PoolableSession extends QdbSession implements Poolable {
        private final Slot slot;

        public PoolableSession(Slot slot) {
            this.slot = slot;
        }

        public void release() {
            slot.release(this);
        }
    }

    private static class SessionAllocator implements Allocator<PoolableSession> {

        public PoolableSession allocate(Slot slot) {
            return new PoolableSession(slot);
        }

        public void deallocate(PoolableSession s) {
            s.close();
        }
    }

    public QdbSessionPool() {
        Config<PoolableSession> config = new Config<PoolableSession>().setAllocator(new SessionAllocator());
        this.pool = new BlazePool<PoolableSession>(config);
    }

    /**
     * Acquire a new QdbSession with a default timeout of 1 second. Ensure to call
     * release() after being finished using a session.
     */
    public PoolableSession claim() throws InterruptedException  {
        return this.claim(new Timeout(1, TimeUnit.SECONDS));
    }

    /**
     * Acquire a new QdbSession with a customizeable timeout. Ensure to call release()
     * after being finished using a session.
     */
    public PoolableSession claim(Timeout timeout) throws InterruptedException {
        return this.pool.claim(timeout);
    }
}
