package net.quasardb.qdb;

/**
 * Exception thrown when the operation cannot be performed because the 64-bit integer would overflow
 */
public final class QdbOverflowException extends QdbOperationException {

    public QdbOverflowException() {
        super("The operation provokes overflow.");
    }
}
