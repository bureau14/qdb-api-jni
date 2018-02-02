package net.quasardb.qdb.exception;

/**
 * Exception thrown when the operating system caused an error
 */
public class SystemException extends Exception {

    public SystemException(String message) {
        super(message);
    }
}
