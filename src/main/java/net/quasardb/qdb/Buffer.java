package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.lang.AutoCloseable;

import net.quasardb.qdb.exception.BufferClosedException;
import net.quasardb.qdb.jni.*;

public final class Buffer implements AutoCloseable {
    final Session session;
    ByteBuffer buffer;

    protected Buffer(Session session, ByteBuffer buffer) {
        this.session = session;
        this.buffer = buffer;
    }

    static public Buffer wrap(Session session, ByteBuffer buffer) {
        return buffer != null ? new Buffer(session, buffer) : null;
    }

    static public Buffer wrap(Session session, Reference<ByteBuffer> ref) {
        return wrap(session, ref.value);
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    @Override
    public void close() {
        if (buffer != null) {
            qdb.release(session.handle(), buffer);
            buffer = null;
        }
    }

    public ByteBuffer toByteBuffer() {
        this.throwIfClosed();
        session.throwIfClosed();
        return buffer != null ? buffer.duplicate() : null;
    }

    @Override
    public String toString() {
        if (buffer == null || session.isClosed())
            return this.getClass().getName() + "[closed]";
        else
            return this.getClass().getName() + "[size=" + buffer.limit() + "]";
    }

    private void throwIfClosed() {
        if (buffer == null) {
            throw new BufferClosedException();
        }
    }
}
