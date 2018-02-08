package net.quasardb.qdb.exception;

/**
 * Exception thrown when argument passed to a method is incorrect.
 */
public final class InvalidArgumentException extends InputException {

    public InvalidArgumentException() {
        super("The argument is invalid.");
    }

    public InvalidArgumentException(String message) {
        super(message);
    }
}
