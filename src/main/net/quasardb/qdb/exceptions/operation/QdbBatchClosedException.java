package net.quasardb.qdb;

/**
 * Exception thrown when using a closed batch.
 */
public final class QdbBatchClosedException extends QdbOperationException {

    public QdbBatchClosedException() {
        super("Operation cannot be performed because the batch has been closed.");
    }
}
