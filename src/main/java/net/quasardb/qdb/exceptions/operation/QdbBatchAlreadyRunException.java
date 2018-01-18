package net.quasardb.qdb;

/**
 * Exception thrown when modifying a batch that has already been run.
 */
public final class QdbBatchAlreadyRunException extends QdbOperationException {

    public QdbBatchAlreadyRunException() {
        super("Cannot modify a batch that has already been run");
    }
}
