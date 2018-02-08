package net.quasardb.qdb.exception;

/**
 * Exception thrown the response from a remote host cannot be treated.
 */
public final class InvalidReplyException extends ProtocolException {

    public InvalidReplyException() {
        super("Invalid reply from the remote host.");
    }

    public InvalidReplyException(String message) {
        super(message);
    }
}
