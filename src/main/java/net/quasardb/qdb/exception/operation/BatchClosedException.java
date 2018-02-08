package net.quasardb.qdb.exception;

/**
 * Exception thrown when using a closed batch.
 */
public final class BatchClosedException extends OperationException {

    public BatchClosedException() {
        super("Operation cannot be performed because the batch has been closed.");
    }
}
