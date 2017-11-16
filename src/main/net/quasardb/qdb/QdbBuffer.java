package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.lang.AutoCloseable;
import net.quasardb.qdb.jni.*;

public final class QdbBuffer implements AutoCloseable {
    final QdbSession session;
    ByteBuffer buffer;

    protected QdbBuffer(QdbSession session, ByteBuffer buffer) {
        this.session = session;
        this.buffer = buffer;
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
        if (buffer == null)
            throw new QdbBufferClosedException();
    }
}
