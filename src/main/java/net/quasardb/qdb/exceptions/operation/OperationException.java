package net.quasardb.qdb.exception;

/**
 * Exception thrown when an operation caused an error
 */
public class OperationException extends Exception {

    public OperationException(String message) {
        super(message);
    }
}
