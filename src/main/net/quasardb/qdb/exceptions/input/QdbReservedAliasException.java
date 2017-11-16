package net.quasardb.qdb;

/**
 * Exception thrown when the specified alias is reserved for quasardb intenal use.
 */
public final class QdbReservedAliasException extends QdbInputException {

    public QdbReservedAliasException() {
        super("The alias name or prefix is reserved.");
    }
}
