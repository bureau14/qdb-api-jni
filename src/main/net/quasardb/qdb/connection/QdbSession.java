package net.quasardb.qdb;

import java.io.Serializable;
import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import java.nio.ByteBuffer;

/**
 * Defines an easy-to-use interface to the quasardb cluster.
 */
class QdbSession {
    private transient long handle;
    private qdb_cluster_security_options securityOptions;

    public static class SecurityOptions implements Serializable {
        protected String userName;
        protected String userPrivateKey;
        protected String clusterPublicKey;

        public SecurityOptions (String userName,
                                String userPrivateKey,
                                String clusterPublicKey) {
            this.userName = userName;
            this.userPrivateKey = userPrivateKey;
            this.clusterPublicKey = clusterPublicKey;
        }

        static qdb_cluster_security_options toNative(SecurityOptions options) {
          return new qdb_cluster_security_options(options.userName,
                                                  options.userPrivateKey,
                                                  options.clusterPublicKey);
        }
    }

    /**
     * Initialize a new QdbSession without security settings. Connections to the
     * QuasarDB cluster will be insecure and unauthenticated.
     */
    public QdbSession() {
        handle = qdb.open_tcp();
    }

    /**
     * Initialize a new QdbSession with security settings. Connections to the
     * QuasarDB will use a secure connection and will be authenticated.
     */
    public QdbSession(SecurityOptions securityOptions) {
        this.securityOptions = SecurityOptions.toNative(securityOptions);
        handle = qdb.open_tcp();
    }

    /**
     * Establishes a connection
     */
    public void connect(String uri) {
        if (this.securityOptions == null) {
            int err = qdb.connect(this.handle, uri);
            QdbExceptionFactory.throwIfError(err);
        } else {
            int err = qdb.secure_connect(this.handle, uri, securityOptions);
            QdbExceptionFactory.throwIfError(err);
        }
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
