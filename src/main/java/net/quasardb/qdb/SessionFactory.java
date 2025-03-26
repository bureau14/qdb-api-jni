package net.quasardb.qdb;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.jni.*;

/**
 * Factory for Sessions.
 *
 * This class is thread-safe.
 */
public class SessionFactory {

    private static final Logger logger = LoggerFactory.getLogger(SessionFactory.class);

    private String qdbUri;
    private Optional<Session.SecurityOptions> securityOptions;
    private Optional<Long> inputBufferSize;
    private Optional<Long> softMemoryLimit;

    public SessionFactory(String qdbUri) {
        this.qdbUri = qdbUri;

        this.securityOptions      = Optional.empty();
        this.inputBufferSize      = Optional.empty();
        this.softMemoryLimit      = Optional.empty();
    }

    public SessionFactory securityOptions(Session.SecurityOptions securityOptions) {
        this.securityOptions = Optional.of(securityOptions);
        return this;
    }

    public SessionFactory inputBufferSize(Long inputBufferSize) {
        this.inputBufferSize = Optional.of(inputBufferSize);
        return this;
    }

    public SessionFactory softMemoryLimit(Long softMemoryLimit) {
        this.softMemoryLimit = Optional.of(softMemoryLimit);
        return this;
    }

    public Session newSession() {

        Session.Builder builder = Session.builder();

        if (this.securityOptions.isPresent()) {
            builder = builder.securityOptions(this.securityOptions.get());
        }

        if (this.inputBufferSize.isPresent()) {
            builder = builder.inputBufferSize(this.inputBufferSize.get());
        }

        if (this.softMemoryLimit.isPresent()) {
            builder = builder.softMemoryLimit(this.softMemoryLimit.get());
        }

        return builder.build();
    }

}
