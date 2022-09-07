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
    private Optional<Long> clientMaxParallelism;
    private Optional<Long> softMemoryLimit;

    public SessionFactory(String qdbUri) {
        this.qdbUri = qdbUri;

        this.securityOptions      = Optional.empty();
        this.inputBufferSize      = Optional.empty();
        this.clientMaxParallelism = Optional.empty();
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

    public SessionFactory clientMaxParallelism(Long clientMaxParallelism) {
        this.clientMaxParallelism = Optional.of(clientMaxParallelism);
        return this;
    }

    public SessionFactory softMemoryLimit(Long softMemoryLimit) {
        this.softMemoryLimit = Optional.of(softMemoryLimit);
        return this;
    }

    public Session newSession() {
        Session ret = null;

        if (this.securityOptions.isPresent()) {
            ret = Session.connect(this.securityOptions.get(),
                                  this.qdbUri);
        } else {
            ret = Session.connect(this.qdbUri);
        }
        assert(ret != null);

        if (this.inputBufferSize.isPresent()) {
            ret.setInputBufferSize(this.inputBufferSize.get().longValue());
        }

        if (this.clientMaxParallelism.isPresent()) {
            ret.setClientMaxParallelism(this.clientMaxParallelism.get().longValue());
        }

        if (this.softMemoryLimit.isPresent()) {
            ret.setSoftMemoryLimit(this.softMemoryLimit.get().longValue());
        }

        return ret;
    }

}
