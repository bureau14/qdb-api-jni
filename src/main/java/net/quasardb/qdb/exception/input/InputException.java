package net.quasardb.qdb.exception;

/**
 * Exception thrown when the input of a command caused an error.
 */
public class InputException extends Exception {

    public InputException(String message) {
        super(message);
    }
}
