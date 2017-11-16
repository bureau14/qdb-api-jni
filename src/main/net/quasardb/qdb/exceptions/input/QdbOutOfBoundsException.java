package net.quasardb.qdb;

/**
 * Exception thrown when an index is out of range.
 */
public final class QdbOutOfBoundsException extends QdbInputException {

    public QdbOutOfBoundsException() {
        super("The given index was out of bounds.");
    }
}
