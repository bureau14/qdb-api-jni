package net.quasardb.qdb;

import java.io.Serializable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.lang.AutoCloseable;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.time.Instant;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.ClusterClosedException;
import net.quasardb.qdb.exception.InputBufferTooSmallException;


/**
 * Represents a connection with the QuasarDB cluster. This class is not
 * thread-safe. As instantiations of this class are expensive (especially
 * when secure connections are used), you are encouraged to pool instances
 * of this class are you would do with any other connection pool.
 */
public class Session implements AutoCloseable {
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

        /**
         * Create security credentials using QuasarDB's credential files as input.
         *
         * @param userSecurityFile Path to the user's security file, e.g. /home/myuser/myuser_private.key
         * @param userSecurityFile Path to the cluster's public key file, e.g. /usr/share/qdb/cluster_public.key
         */
        public static SecurityOptions ofFiles(String userSecurityFile,
                                              String clusterPublicKeyFile) throws IOException {
            String clusterPublicKey = new String(Files.readAllBytes(FileSystems.getDefault().getPath(clusterPublicKeyFile)));
            String userSecurityJson = new String(Files.readAllBytes(FileSystems.getDefault().getPath(userSecurityFile)));

            // Using Regex is not pretty, but since our security files are always
            // created by us and very simple, using this avoids pulling in a third-party
            // library just for parsing this JSON.
            Matcher userNameMatcher =
                Pattern.compile("\"username\"\\s*:\\s*\"([^,]*)\"").matcher(userSecurityJson);
            Matcher userSecretKeyMatcher =
                Pattern.compile("\"secret_key\"\\s*:\\s*\"([^,]*)\"").matcher(userSecurityJson);

            if (!userNameMatcher.find() || !userSecretKeyMatcher.find()) {

                throw new RuntimeException("Unable to parse user security file");
            }

            String userName = userNameMatcher.group(1);
            String userSecretKey = userSecretKeyMatcher.group(1);

            return new SecurityOptions (userName,
                                        userSecretKey,
                                        clusterPublicKey);

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
        qdb.connect(s.handle, uri);

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
        qdb.secure_connect(s.handle, uri, SecurityOptions.toNative(options));

        return s;
    }

    public void close() {
        if (handle != 0) {
            logger.info("Closing session");
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
            throw new ClusterClosedException("Session function invoked but is already closed.");
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

        qdb.option_set_timeout(handle, timeoutMillis);
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

        qdb.option_set_client_max_in_buf_size(handle, size);
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


    /**
     * Set maximum client parallelism for this session.
     *
     * @param threadCount The desired maximum number of threads to use for query execution, or 0
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public void setClientMaxParallelism(long threadCount) throws ClusterClosedException {
        throwIfClosed();

        qdb.option_set_client_max_parallelism(handle, threadCount);
    }

    /**
     * Returns the current input buffer size (in bytes).
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public long getClientMaxParallelism() throws ClusterClosedException {
        throwIfClosed();

        return qdb.option_get_client_max_parallelism(handle);
    }

    /**
     * Set maximum number of connections per qdbd host.
     *
     * @param limit The limit of the number of connections
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public void setConnectionPerAddressSoftLimit(long limit) throws ClusterClosedException {
        throwIfClosed();

        qdb.option_set_connection_per_address_soft_limit(handle, limit);
    }

    /**
     * Returns the current connection limit per qdbd host.
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public long getConnectionPerAddressSoftLimit() throws ClusterClosedException {
        throwIfClosed();

        return qdb.option_get_connection_per_address_soft_limit(handle);
    }

    /**
     * Set the maximum "load" of a single execution batch per thread.
     *
     * @param batchLoad The maximum load of a batch
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public void setMaxBatchLoad(long batchLoad) throws ClusterClosedException {
        throwIfClosed();

        qdb.option_set_client_max_batch_load(handle, batchLoad);
    }

    /**
     * Returns the maximum load of a single execution batch per thread.
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public long getMaxBatchLoad() throws ClusterClosedException {
        throwIfClosed();

        return qdb.option_get_client_max_batch_load(handle);
    }

    /**
     * Sets the soft memory limit of the client.
     *
     * This sets the desired limit of the off-heap memory buffer the QuasarDB C API will
     * maintain.
     *
     * @param limit The desired soft limit (in bytes)
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public long setSoftMemoryLimit(long limit) throws ClusterClosedException {
        assert(limit > 0);
        throwIfClosed();

        return qdb.option_set_client_soft_memory_limit(handle, limit);
    }

    /**
     * Returns information about the current memory usage.
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public String getMemoryInfo() throws ClusterClosedException {
        throwIfClosed();

        return qdb.option_get_client_memory_info(handle);
    }

    /**
     * Logs memory usage information through SLF4J facade with DEBUG log level.
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public void logMemoryInfo() throws ClusterClosedException {
        throwIfClosed();

        logger.debug(getMemoryInfo());
    }

    /**
     * Cleans up memory allocator and purged any unused cache. Acquires a global lock
     * on the memory allocator and temporarily pauses all threads, use with caution.
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public long tidyMemory() throws ClusterClosedException {
        throwIfClosed();

        logger.warn("tidying memory");
        Instant start = Instant.now();
        long ret = qdb.option_client_tidy_memory(handle);
        Instant stop = Instant.now();

        logger.info("successfully tidied memory in {}ms", Duration.between(start, stop).toMillis());
        return ret;
    }

    /**
     * Wait for all nodes of the cluster to be stabilized.
     *
     * @param timeoutMillis The timeout of the operation, in milliseconds
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public void waitForStabilization(int timeoutMillis) throws ClusterClosedException {
        throwIfClosed();
        qdb.wait_for_stabilization(handle, timeoutMillis);
    }

    /**
     * Purge all data from cluster. Useful for integration testing.
     *
     * @param timeoutMillis The timeout of the operation, in milliseconds
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public void purgeAll(int timeoutMillis) throws ClusterClosedException {
        logger.warn("Purging entire cluster");
        throwIfClosed();
        qdb.purge_all(handle, timeoutMillis);
    }
}
