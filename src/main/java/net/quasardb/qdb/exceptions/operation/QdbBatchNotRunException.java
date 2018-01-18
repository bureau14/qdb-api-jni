package net.quasardb.qdb;

/**
 * Exception thrown when reading the result of a batch that hasn't been run.
 */
public final class QdbBatchNotRunException extends QdbOperationException {

    public QdbBatchNotRunException() {
        super("Cannot read the result because the batch hasn't been run.");
    }
}
