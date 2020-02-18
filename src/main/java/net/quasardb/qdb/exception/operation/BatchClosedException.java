package net.quasardb.qdb.exception;

/**
 * Exception thrown when using a closed batch.
 */
public final class BatchClosedException extends OperationException {

    public BatchClosedException(String message) {
        super(message);
    }
}
