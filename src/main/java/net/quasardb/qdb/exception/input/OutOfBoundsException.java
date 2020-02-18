package net.quasardb.qdb.exception;

/**
 * Exception thrown when an index is out of range.
 */
public final class OutOfBoundsException extends InputException {

    public OutOfBoundsException(String message) {
        super(message);
    }
}
