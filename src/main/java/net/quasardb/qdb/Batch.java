package net.quasardb.qdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.Session;

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
