package net.quasardb.qdb.exception;

/**
 * Exception thrown when the specified entry has a type incompatible for this operation.
 */
public final class IncompatibleTypeException extends OperationException {

    public IncompatibleTypeException() {
        super("The alias has a type incompatible for this operation.");
    }
}
