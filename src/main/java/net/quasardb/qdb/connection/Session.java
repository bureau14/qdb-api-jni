package net.quasardb.qdb;

import java.io.Serializable;
import java.nio.ByteBuffer;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.exception.ClusterClosedException;


/**
 * Represents a connection with the QuasarDB cluster. This class is backed
 * by a connection pool, and is thread-safe. You are encouraged to reuse
 * this object as much as possible, ideally only creating a single session
 * for the lifetime of your process.
 */
public class Session {
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
     * Initialize a new Session without security settings. Connections to the
     * QuasarDB cluster will be insecure and unauthenticated.
     */
    public Session() {
        handle = qdb.open_tcp();
    }

    /**
     * Initialize a new Session with security settings. Connections to the
     * QuasarDB will use a secure connection and will be authenticated.
     */
    public Session(SecurityOptions securityOptions) {
        this.securityOptions = SecurityOptions.toNative(securityOptions);
        handle = qdb.open_tcp();
    }

    /**
     * Establishes a connection
     *
     * @param uri Fully qualified quasardb cluster uri, e.g. qdb://127.0.0.1:2836
     * @return A QuasarDB session
     */
    static public Session connect(String uri) {
        Session s = new Session();
        int err = qdb.connect(s.handle, uri);
        ExceptionFactory.throwIfError(err);

        return s;
    }

    /**
     * Establishes a secure connection
     *
     * @param options Security options for authenticating with cluster
     * @param uri Fully qualified quasardb cluster uri, e.g. qdb://127.0.0.1:2836
     * @return A secure QuasarDB session
     */
    static public Session connect(SecurityOptions options, String uri) {
        Session s = new Session();
        int err = qdb.secure_connect(s.handle, uri, SecurityOptions.toNative(options));
        ExceptionFactory.throwIfError(err);

        return s;
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
            throw new ClusterClosedException();
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    public long handle() {
        return handle;
    }
}
