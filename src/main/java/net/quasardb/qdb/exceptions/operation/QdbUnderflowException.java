package net.quasardb.qdb;

/**
 * Exception thrown when the operation cannot be performed because the 64-bit integer would underflow
 */
public final class QdbUnderflowException extends QdbOperationException {

    public QdbUnderflowException() {
        super("The operation provokes underflow.");
    }
}
