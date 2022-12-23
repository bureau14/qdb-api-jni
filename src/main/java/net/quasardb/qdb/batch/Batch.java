package net.quasardb.qdb.batch;

import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.Session;

/**
 * Batches multiple operations into a single atomic operation.
 *
 * Improves efficiency by avoiding many round-trips from client to server by using
 * a single large operation.
 *
 * Enables better consistency by atomically applying all operations in a single
 * batch: either all operations succeed, or none do.
 */
public final class Batch implements AutoCloseable {
    protected abstract static class Operation {
        public abstract void process(long handle, long batch, int index);
        public Object result;
        public int error;
    }

    public enum CommitMode {
        FAST,
        TRANSACTIONAL
    }


    /**
     * Batch options.
     */
    static public class Options {
        private CommitMode commitMode;

        public Options() {
            this.commitMode = CommitMode.FAST;
        };

        /**
         * Resets commit mode to 'fast', i.e. the batch execution is optimized for performance
         * and each operation is run independently.
         */
        public void enableFastCommit() {
            this.commitMode = CommitMode.FAST;
        };


        /**
         * Resets commit mode to 'transactional', i.e. the batch is executed in order as a
         * single atomic transaction.
         */
        public void enableTransactionalCommit() {
            this.commitMode = CommitMode.TRANSACTIONAL;
        };

        /**
         * Resets commit mode to provided value.
         */
        public void setCommitMode(CommitMode commitMode) {
            this.commitMode = commitMode;
        }

        /**
         * Provides access to the commit mode.
         */
        public CommitMode getCommitMode() {
            return this.commitMode;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Batch.class);
    final Session session;
    final Options options;

    List<Operation> ops = new ArrayList<Operation>();


    /**
     * Construct a new Batch.
     */
    protected Batch(Session session, Options options) {
        this.session = session;
        this.options = options;
    }

    /**
     * Create a builder instance.
     *
     * @param session Active connection with the QuasarDB cluster.
     */
    public static Builder builder(Session session) {
        return new Builder(session);
    };

    /**
     * Enqueue a batched operation.
     */
    void add(Operation op) {
        this.ops.add(op);
    }

    /**
     * Returns the amount of operations in this batch.
     */
    public int size() {
        return this.ops.size();
    }

    /**
     * Returns true if there are no operations in the batch.
     */
    public boolean isEmpty() {
        return this.ops.isEmpty();
    }


    /**
     * Get access to the underlying session object.
     */
    public Session session() {
        return this.session;
    }

    /**
     * Explicitly close batch.
     */
    public void close() {
        logger.info("Closing batch");
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    public BlobEntry blob(String alias) {
        return BlobEntry.ofAlias(this, alias);
    }

    public void commit() {

        // This is where the 'magic' happens, and we'll invoke our native functions
        // to construct the qdb_batch structure(s).

        int n = this.ops.size();

        // `batch` is a pointer
        long batch = qdb.init_batch(this.session.handle(), n);

        try {

            logger.debug("Adding {} operations to batch", n);

            int idx = 0;
            for (Operation op : this.ops) {
                // Each operation's `.process()` function invoces a native JNI function
                // for adding the batched operaton to the queue.
                op.process(this.session.handle(), batch, idx++);
            }

            logger.debug("Committing batch");

            // Commits
            int count = -1;
            switch (this.options.commitMode) {
            case FAST:
                count = qdb.commit_batch_fast(this.session.handle(), batch, n);
                break;
            case TRANSACTIONAL:
                count = qdb.commit_batch_transactional(this.session.handle(), batch, n);
                break;
            }

            logger.debug("Successfully ran {} operations", count);

            this.ops.clear();
        } catch (Throwable t) {
            qdb.release_batch(this.session.handle(), batch);
        }
    }



    public static final class Builder {
        private Session session;
        private Batch.Options options;

        protected Builder(Session session) {
            this.session = session;
            this.options = new Batch.Options();
        };

        public Builder commitMode(Batch.CommitMode commitMode) {
            this.options.setCommitMode(commitMode);
            return this;
        }

        public Builder fastCommit() {
            this.options.enableFastCommit();
            return this;
        }

        public Builder transactionalCommit() {
            this.options.enableTransactionalCommit();
            return this;
        }

        public Batch build() {
            return new Batch(this.session, this.options);
        }
    }

}
