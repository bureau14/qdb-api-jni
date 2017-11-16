package net.quasardb.qdb;

/**
 * Exception thrown when the local (ie client) operating system caused an error
 */
public class QdbLocalSystemException extends QdbSystemException {

    public QdbLocalSystemException(String message) {
        super(message);
    }
}
