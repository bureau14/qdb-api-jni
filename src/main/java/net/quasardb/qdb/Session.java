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
import java.util.Optional;
import java.time.Instant;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.ClusterClosedException;
import net.quasardb.qdb.exception.InputBufferTooSmallException;


/**
 * Represents a session with the QuasarDB cluster. This class is not
 * thread-safe. As instantiations of this class are expensive (especially
 * when secure connections are used), you are encouraged to pool instances
 * of this class are you would do with any other connection pool.
 */
public class Session implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    private transient long handle;
    private qdb_cluster_security_options securityOptions;

    /**
     * Compression mode representation
     */
    public enum CompressionMode {
        NONE(Constants.qdb_comp_none),
        FAST(Constants.qdb_comp_fast),
        BEST(Constants.qdb_comp_best),
        BALANCED(Constants.qdb_comp_balanced)
        ;

        protected final int mode;

        CompressionMode(int mode) {
            this.mode = mode;
        }

        public int asInt() {
            return this.mode;
        }

        public static CompressionMode fromInt(int mode) {
            switch(mode) {

            case Constants.qdb_comp_none:
                return CompressionMode.NONE;

            case Constants.qdb_comp_fast:
                return CompressionMode.FAST;

            case Constants.qdb_comp_best:
                return CompressionMode.BEST;

            case Constants.qdb_comp_balanced:
                return CompressionMode.BALANCED;
            }

            return CompressionMode.NONE;
        }
    }

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
     * Builder implementation for sessions. Use this class to create new sessions
     * and set (optional) options accordingly.
     */
    public static final class Builder {
        private String uri;
        private Optional<SecurityOptions> securityOptions;

        private Optional<Long> inputBufferSize;
        private Optional<Long> softMemoryLimit;
        private Optional<Integer> connectionPerAddressSoftLimit;
        private Optional<Integer> maxBatchLoad;
        private Optional<CompressionMode> compressionMode;

        protected Builder() {
            this.uri = null;
            this.securityOptions = Optional.empty();

            this.inputBufferSize = Optional.empty();
            this.softMemoryLimit = Optional.empty();
            this.connectionPerAddressSoftLimit = Optional.empty();
            this.maxBatchLoad = Optional.empty();
            this.compressionMode = Optional.of(CompressionMode.BALANCED);
        };

        public Builder uri(String uri) {
            this.uri = uri;

            return this;
        }

        public Builder securityOptions(SecurityOptions options) {
            this.securityOptions = Optional.of(options);

            return this;
        }

        public Builder inputBufferSize(Long inputBufferSize) throws IllegalArgumentException {
            if (inputBufferSize <= 0) {
                throw new IllegalArgumentException("Input buffer size must be > 0");
            }

            this.inputBufferSize = Optional.of(inputBufferSize);

            return this;
        }

        public Builder softMemoryLimit(Long softMemoryLimit) throws IllegalArgumentException {
            if (softMemoryLimit <= 0) {
                throw new IllegalArgumentException("Soft memory limit must be > 0");
            }

            this.softMemoryLimit = Optional.of(softMemoryLimit);

            return this;
        }

        public Builder connectionPerAddressSoftLimit(Integer n) throws IllegalArgumentException {
            if (n <= 0) {
                throw new IllegalArgumentException("Connection per address soft limit must be > 0");
            }

            this.connectionPerAddressSoftLimit = Optional.of(n);

            return this;
        }


        public Builder maxBatchLoad(Integer n) throws IllegalArgumentException {
            if (n <= 0) {
                throw new IllegalArgumentException("Max batch load must be > 0");
            }

            this.maxBatchLoad = Optional.of(n);

            return this;
        }

        public Builder noCompression() {
            this.compressionMode = Optional.of(CompressionMode.NONE);

            return this;
        }

        public Builder fastCompression() {
            this.compressionMode = Optional.of(CompressionMode.FAST);

            return this;
        }

        public Builder bestCompression() {
            this.compressionMode = Optional.of(CompressionMode.BEST);

            return this;
        }

        public Builder balancedCompression() {
            this.compressionMode = Optional.of(CompressionMode.BALANCED);

            return this;
        }

        public Session build() throws IllegalArgumentException {
            if (this.uri == null) {
                throw new IllegalArgumentException("Must always provide a cluster uri");
            }

            Session s = new Session();

            if (this.compressionMode.isPresent()) {
                logger.debug("Setting compression mode to: {}", this.compressionMode.get());
                qdb.option_set_compression(s.handle, this.compressionMode.get().asInt());
            }

            if (this.inputBufferSize.isPresent()) {
                logger.debug("Setting input buffer size to: {}", this.inputBufferSize.get());
                qdb.option_set_client_max_in_buf_size(s.handle, this.inputBufferSize.get().longValue());
            }

            if (this.softMemoryLimit.isPresent()) {
                logger.debug("Setting soft memory limit to: {}", this.softMemoryLimit.get());
                qdb.option_set_client_soft_memory_limit(s.handle, this.softMemoryLimit.get().longValue());
            }

            if (this.maxBatchLoad.isPresent()) {
                logger.debug("Setting max batch load to: {}", this.maxBatchLoad.get());
                qdb.option_set_client_max_batch_load(s.handle, this.maxBatchLoad.get().longValue());
            }

            if (this.connectionPerAddressSoftLimit.isPresent()) {
                logger.debug("Setting connection per address soft limit to: {}", this.connectionPerAddressSoftLimit.get());
                qdb.option_set_connection_per_address_soft_limit(s.handle, this.connectionPerAddressSoftLimit.get().longValue());
            }

            if (this.securityOptions.isPresent()) {
                logger.info("Establishing a secure connection to conluster: {}", this.uri);
                qdb.secure_connect(s.handle, this.uri, SecurityOptions.toNative(this.securityOptions.get()));
            } else {
                logger.info("Establishing an insecure connection to conluster: {}", this.uri);
                qdb.connect(s.handle, this.uri);
            }

            return s;
        };

    };


    /**
     * Open a new session. This is the equivalent of opening a socket but not
     * yet connecting to it.
     */
    public Session() {
        this.handle = qdb.open_tcp();
    }

    /**
     * Create a builder instance.
     *
     * Use this function to create new session objects and connect to the cluster.
     */
    public static Builder builder() {
        return new Builder();
    };

    public void close() {
        if (this.handle != 0) {
            logger.info("Closing session");
            qdb.close(handle);
            this.handle = 0;
        }
    }

    public boolean isClosed() {
        return this.handle == 0;
    }

    public void throwIfClosed() throws ClusterClosedException {
        if (this.handle == 0) {
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
        return this.handle;
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
     * Returns the current connection limit per qdbd host.
     *
     * @throws ClusterClosedException If the connection to the cluster is currently closed.
     */
    public long getConnectionPerAddressSoftLimit() throws ClusterClosedException {
        throwIfClosed();

        return qdb.option_get_connection_per_address_soft_limit(handle);
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
