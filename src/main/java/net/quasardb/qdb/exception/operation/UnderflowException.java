package net.quasardb.qdb.exception;

/**
 * Exception thrown when the operation cannot be performed because the 64-bit integer would underflow
 */
public final class UnderflowException extends OperationException {

    public UnderflowException(String message) {
        super(message);
    }
}
