package net.quasardb.qdb;

/**
 * Exception thrown when the connection to the database caused an error
 */
public class QdbConnectionException extends QdbException {

    public QdbConnectionException(String message) {
        super(message);
    }
}
