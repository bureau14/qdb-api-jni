package net.quasardb.qdb.exception;

/**
 * Exception thrown when modifying a batch that has already been run.
 */
public final class BatchAlreadyRunException extends OperationException {

    public BatchAlreadyRunException(String message) {
        super(message);
    }
}
