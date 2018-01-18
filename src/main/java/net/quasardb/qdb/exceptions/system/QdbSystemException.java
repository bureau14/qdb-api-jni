package net.quasardb.qdb;

/**
 * Exception thrown when the operating system caused an error
 */
public class QdbSystemException extends QdbException {

    public QdbSystemException(String message) {
        super(message);
    }
}
