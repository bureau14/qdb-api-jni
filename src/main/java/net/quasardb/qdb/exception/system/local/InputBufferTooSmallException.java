package net.quasardb.qdb.exception;

/**
 * Exception is thrown when the local input buffer is unable to hold the entire
 * resultset. If you encounter this error, either request a smaller timespan per
 * query or increase your local client buffer size.
 */
public class InputBufferTooSmallException extends LocalSystemException {

    public InputBufferTooSmallException() {
        super("Local input buffer is too small to fit the result set. Hint: consider increasing your local buffer size using Session.setInputBufferSize() or requesting smaller timeranged.");
    }
}
