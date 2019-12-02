package net.quasardb.qdb;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.exception.ClusterClosedException;
import net.quasardb.qdb.exception.InputBufferTooSmallException;


/**
 * Represents a connection with the QuasarDB cluster. This class is not
 * thread-safe. As instantiations of this class are expensive (especially
 * when secure connections are used), you are encouraged to pool instances
 * of this class are you would do with any other connection pool.
 */
public class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    private transient long handle;
    private qdb_cluster_security_options securityOptions;

    /**
     * Optional configuration for establishing a secure connection.
     */
    public static class SecurityOptions implements Serializable {
        protected String userName;
        protected String userPrivateKey;
        protected String clusterPublicKey;

        /**
         * @param userName Username to use when authenticating to the cluster.
         * @param userPrivateKey Private key of the user.
         * @param clusterPublicKey Public key of the cluster.
         */
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
        logger.info("Establishing an insecure connection to cluster: {}", uri);
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
        logger.info("Establishing a secure connection to cluster: {}", uri);
        Session s = new Session();
        int err = qdb.secure_connect(s.handle, uri, SecurityOptions.toNative(options));
        ExceptionFactory.throwIfError(err);

        return s;
    }

    public void close() {
        logger.info("Closing session");
        if (handle != 0) {
            qdb.close(handle);
            handle = 0;
        }
    }

    public boolean isClosed() {
        return handle == 0;
    }

    public void throwIfClosed() {
        if (handle == 0) {
            logger.warn("Session invoked while closed!");
            throw new ClusterClosedException();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    public long handle() {
        return handle;
    }


    /**
     * Set network timeout for this session.
     *
     * @param timeoutMillis The timeout of the operation, in milliseconds
     *
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     */
    public void setTimeout(int timeoutMillis) throws ClusterClosedException {
        throwIfClosed();

        int err = qdb.option_set_timeout(handle, timeoutMillis);
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Set input buffer size for this session. Increase this if you encounter
     * {@link InputBufferTooSmallException} while retrieving data from the server.
     *
     * @param size The desired size (in bytes) of the input buffer.
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public void setInputBufferSize(long size) throws ClusterClosedException {
        throwIfClosed();

        int err = qdb.option_set_client_max_in_buf_size(handle, size);
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Returns the current input buffer size (in bytes).
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public long getInputBufferSize() throws ClusterClosedException {
        throwIfClosed();

        return qdb.option_get_client_max_in_buf_size(handle);
    }
}
