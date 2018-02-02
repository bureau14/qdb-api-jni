package net.quasardb.qdb.exception;

/**
 * Exception thrown when the remote (ie server) operating system caused an error
 */
public class RemoteSystemException extends SystemException {

    public RemoteSystemException(String message) {
        super(message);
    }
}
