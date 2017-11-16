package net.quasardb.qdb;

/**
 * Exception thrown when an error is detected in the quasardb protocol
 */
public class QdbProtocolException extends QdbException {

    public QdbProtocolException(String message) {
        super(message);
    }
}
