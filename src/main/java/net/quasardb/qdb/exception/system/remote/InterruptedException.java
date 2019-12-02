package net.quasardb.qdb.exception;

/**
 * Exception is thrown when the remote connection has been interrupted. This may
 * or may not be recoverable.
 */
public class InterruptedException extends RemoteSystemException {

    public InterruptedException() {
        super("The remote connection was interrupted.");
    }
}
