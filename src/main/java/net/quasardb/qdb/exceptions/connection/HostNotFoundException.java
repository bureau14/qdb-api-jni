package net.quasardb.qdb.exception;

/**
 * Exception thrown when the host name resolution fails.
 */
public final class HostNotFoundException extends ConnectionException {

    public HostNotFoundException() {
        super("The remote host cannot be resolved.");
    }
}
