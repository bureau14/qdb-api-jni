package net.quasardb.qdb.exception;

/**
 * Exception thrown when an index is out of range.
 */
public final class OutOfBoundsException extends InputException {

    public OutOfBoundsException() {
        super("The given index was out of bounds.");
    }
}
