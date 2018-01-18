package net.quasardb.qdb;

/**
 * Exception thrown when the input of a command caused an error.
 */
public class QdbInputException extends QdbException {

    public QdbInputException(String message) {
        super(message);
    }
}
