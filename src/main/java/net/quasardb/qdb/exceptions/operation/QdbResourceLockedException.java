package net.quasardb.qdb;

/**
 * Exception thrown when an operation cannot be performed because the entry is locked.
 */
public final class QdbResourceLockedException extends QdbOperationException {

    public QdbResourceLockedException() {
        super("The entry is currently locked by another client.");
    }
}
