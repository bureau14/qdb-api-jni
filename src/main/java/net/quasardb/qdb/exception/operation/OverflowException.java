package net.quasardb.qdb.exception;

/**
 * Exception thrown when the operation cannot be performed because the 64-bit integer would overflow
 */
public final class OverflowException extends OperationException {

    public OverflowException() {
        super("The operation provokes overflow.");
    }
}
