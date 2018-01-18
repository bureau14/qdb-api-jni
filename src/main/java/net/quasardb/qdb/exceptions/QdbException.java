package net.quasardb.qdb;

/**
 * Exception thrown when an entry cannot be found in the database
 */
public class QdbException extends RuntimeException {

    public QdbException(String message) {
        super(message);
    }
}
