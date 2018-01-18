package net.quasardb.qdb;

/**
 * Exception thrown when argument passed to a method is incorrect.
 */
public final class QdbInvalidArgumentException extends QdbInputException {

    public QdbInvalidArgumentException() {
        super("The argument is invalid.");
    }

    public QdbInvalidArgumentException(String message) {
        super(message);
    }
}
