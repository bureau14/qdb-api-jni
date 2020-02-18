package net.quasardb.qdb.exception;

/**
 * Exception thrown when an operation cannot be performed because the entry is locked.
 */
public final class ResourceLockedException extends OperationException {

    public ResourceLockedException(String message) {
        super(message);
    }
}
