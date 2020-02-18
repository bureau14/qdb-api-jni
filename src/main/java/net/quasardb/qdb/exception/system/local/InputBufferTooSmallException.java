package net.quasardb.qdb.exception;

/**
 * Exception is thrown when the local input buffer is unable to hold the entire
 * resultset. If you encounter this error, either request a smaller timespan per
 * query or increase your local client buffer size.
 */
public class InputBufferTooSmallException extends LocalSystemException {

    public InputBufferTooSmallException(String message) {
        super(message);
    }
}
