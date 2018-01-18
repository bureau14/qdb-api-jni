package net.quasardb.qdb;

/**
 * Exception thrown the response from a remote host cannot be treated.
 */
public final class QdbInvalidReplyException extends QdbProtocolException {

    public QdbInvalidReplyException() {
        super("Invalid reply from the remote host.");
    }

    public QdbInvalidReplyException(String message) {
        super(message);
    }
}
