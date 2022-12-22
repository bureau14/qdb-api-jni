package net.quasardb.qdb.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(Batch.class);
    final Session session;

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




}
