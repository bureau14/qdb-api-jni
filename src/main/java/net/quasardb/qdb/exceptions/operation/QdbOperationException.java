package net.quasardb.qdb;

/**
 * Exception thrown when an operation caused an error
 */
public class QdbOperationException extends QdbException {

    public QdbOperationException(String message) {
        super(message);
    }
}
