package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.nio.ByteBuffer;

final class QdbSession {
    private transient long handle;

    public QdbSession() {
        handle = qdb.open_tcp();
    }

    public void close() {
        if (handle != 0) {
            qdb.close(handle);
            handle = 0;
        }
    }

    public boolean isClosed() {
        return handle == 0;
    }

    public void throwIfClosed() {
        if (handle == 0)
            throw new QdbClusterClosedException();
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    public long handle() {
        return handle;
    }

    public QdbBuffer wrapBuffer(Reference<ByteBuffer> ref) {
        return wrapBuffer(ref.value);
    }

    private QdbBuffer wrapBuffer(ByteBuffer buffer) {
        return buffer != null ? new QdbBuffer(this, buffer) : null;
    }
}
