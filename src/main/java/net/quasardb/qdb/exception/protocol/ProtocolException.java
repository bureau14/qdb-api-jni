package net.quasardb.qdb.exception;

/**
 * Exception thrown when an error is detected in the quasardb protocol
 */
public class ProtocolException extends Exception {

    public ProtocolException(String message) {
        super(message);
    }
}
