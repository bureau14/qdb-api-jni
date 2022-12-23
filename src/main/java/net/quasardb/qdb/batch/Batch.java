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

    private static final Logger logger = LoggerFactory.getLogger(Batch.class);
    final Session session;
    List<Operation> ops = new ArrayList<Operation>();


    /**
     * Construct a new Batch.
     */
    protected Batch(Session session) {
        this.session = session;
    }

    /**
     * Construct a new Batch.
     */
    public static Batch create(Session session) {
        return new Batch(session);
    }

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

        int n = this.ops.size();
        long batch = createBatch(this.session, n);

        try {

            logger.debug("Adding {} operations to batch", n);

            int idx = 0;
            for (Operation op : this.ops) {
                op.process(this.session.handle(), batch, idx++);
            }

            logger.debug("Committing batch");

            int count = qdb.run_batch(this.session.handle(), batch, n);

            logger.debug("Successfully ran {} operations", count);

            this.ops.clear();
        } catch (Throwable t) {
            releaseBatch(this.session, batch);
        }
    }

    private static long createBatch(Session s, int n) {
        logger.info("Creating batch for {} operations", n);

        Reference<Long> batch = new Reference<Long>();
        qdb.init_operations(s.handle(), n, batch);
        return batch.value;
    }

    private static void releaseBatch(Session session, long batch) {
        logger.debug("Releasing batch");
        qdb.delete_batch(session.handle(), batch);
    }


}
