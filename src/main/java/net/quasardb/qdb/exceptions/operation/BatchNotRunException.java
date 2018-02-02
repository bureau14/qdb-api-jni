package net.quasardb.qdb.exception;

/**
 * Exception thrown when reading the result of a batch that hasn't been run.
 */
public final class BatchNotRunException extends OperationException {

    public BatchNotRunException() {
        super("Cannot read the result because the batch hasn't been run.");
    }
}
