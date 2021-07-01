package net.quasardb.qdb;

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
    private Session.SecurityOptions securityOptions;

    public SessionFactory(String qdbUri) {
        this.qdbUri = qdbUri;
    }

    public SessionFactory(String qdbUri, String qdbUser, String qdbPrivateKey, String qdbPublicKey) {
        this(qdbUri, new Session.SecurityOptions(qdbUser,
                                                 qdbPrivateKey,
                                                 qdbPublicKey));
    }

    public SessionFactory(String qdbUri, Session.SecurityOptions securityOptions) {
        this.qdbUri = qdbUri;
        this.securityOptions = securityOptions;
    }

    public Session newSession() {
        if (this.securityOptions != null) {
            return Session.connect(this.securityOptions,
                                   this.qdbUri);
        } else {
            return Session.connect(this.qdbUri);
        }
    }
}
