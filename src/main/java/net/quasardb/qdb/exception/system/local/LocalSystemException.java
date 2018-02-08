package net.quasardb.qdb.exception;

/**
 * Exception thrown when the local (ie client) operating system caused an error
 */
public class LocalSystemException extends SystemException {

    public LocalSystemException(String message) {
        super(message);
    }
}
