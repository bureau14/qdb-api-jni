package net.quasardb.qdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.jni.*;

class SessionFactory {

    private static final Logger logger = LoggerFactory.getLogger(SessionFactory.class);

    private String qdbUri;
    private Session.SecurityOptions securityOptions;

    public SessionFactory(String qdbUri) {
        this.qdbUri = qdbUri;
    }

    public SessionFactory(String qdbUri, String qdbUser, String qdbPrivateKey, String qdbPublicKey) {
        this.qdbUri = qdbUri;
        this.securityOptions = new Session.SecurityOptions(qdbUser,
                                                           qdbPrivateKey,
                                                           qdbPublicKey);
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
