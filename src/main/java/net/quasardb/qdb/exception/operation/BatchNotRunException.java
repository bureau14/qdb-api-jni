package net.quasardb.qdb.exception;

/**
 * Exception thrown when reading the result of a batch that hasn't been run.
 */
public final class BatchNotRunException extends OperationException {

    public BatchNotRunException(String message) {
        super(message);
    }
}
