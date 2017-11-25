package net.quasardb.qdb;

import java.util.concurrent.TimeUnit;
import stormpot.*;
import net.quasardb.qdb.*;

/**
 * Provides an object pool which allows for sharing and re-use of quasardb
 * connections over a period of time.
 */
class QdbSessionPool implements AutoCloseable {

    /**
     * Keeps track of all the currently allocated sessions.
     */
    private BlazePool<PoolableSession> pool;

    /**
     * A QdbSession that provides the interface necessary for connection pool
     * management. The user of this class must be careful to call release()
     * after they are done with the class.
     */
    public static class PoolableSession extends QdbSession implements Poolable {
        private final Slot slot;

        public PoolableSession( Slot slot) {
            super();
            this.slot = slot;
        }

        public PoolableSession(QdbSession.SecurityOptions securityOptions, Slot slot) {
            super(securityOptions);
            this.slot = slot;
        }

        public void release() {
            slot.release(this);
        }
    }

    private static class SessionAllocator implements Allocator<PoolableSession> {
        private String uri;

        SessionAllocator(String uri) {
            this.uri = uri;
        }

        public PoolableSession allocate(Slot slot) {
            PoolableSession s = new PoolableSession(slot);
            s.connect(uri);
            return s;
        }

        public void deallocate(PoolableSession s) {
            s.close();
        }
    }

    private static class SecureSessionAllocator implements Allocator<PoolableSession> {
        private String uri;
        private QdbSession.SecurityOptions securityOptions;

        SecureSessionAllocator(String uri, QdbSession.SecurityOptions securityOptions) {
            this.uri = uri;
            this.securityOptions = securityOptions;
        }

        public PoolableSession allocate(Slot slot) {
            PoolableSession s = new PoolableSession(securityOptions, slot);
            s.connect(uri);
            return s;
        }

        public void deallocate(PoolableSession s) {
            s.close();
        }
    }

    public QdbSessionPool(String uri) {
        Config<PoolableSession> config = new Config<PoolableSession>().setAllocator(new SessionAllocator(uri));
        this.pool = new BlazePool<PoolableSession>(config);
    }

    public QdbSessionPool(String uri, QdbSession.SecurityOptions securityOptions) {
        Config<PoolableSession> config =
            new Config<PoolableSession>().setAllocator(new SecureSessionAllocator(uri, securityOptions));
        this.pool = new BlazePool<PoolableSession>(config);
    }

    /**
     * Acquire a new QdbSession with a default timeout of 1 second. Ensure to call
     * release() after being finished using a session.
     */
    public PoolableSession claim()  {
        return this.claim(new Timeout(15, TimeUnit.SECONDS));
    }

    /**
     * Acquire a new QdbSession with a customizeable timeout. Ensure to call release()
     * after being finished using a session.
     *
     * @throws QdbConnectionException If a session could not be acquired from the pool.
     */
    public PoolableSession claim(Timeout timeout) {

        try {
            PoolableSession s = this.pool.claim(timeout);
            s.throwIfClosed();

            if (s == null) {
                throw new QdbConnectionException("A timeout occurred while acquiring a connection from the pool.");
            }

            return s;
        } catch (InterruptedException e) {
            throw new QdbConnectionException("Thread was interrupted while acquiring a connection from the pool.");
        }
    }

    public void close() {
        this.pool.shutdown();
    }
}
