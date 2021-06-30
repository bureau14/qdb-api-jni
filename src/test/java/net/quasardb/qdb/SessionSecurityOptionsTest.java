package net.quasardb.qdb;

import java.io.IOException;

import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import net.quasardb.qdb.*;

public class SessionSecurityOptionsTest {

    @Test
    public void canCreateSecurityOptions () {
        Session.SecurityOptions so =
            new Session.SecurityOptions("test-user",
                                        "S0huLfhqH+046Hy9UYAY6r4N2/P7Hbeq94FyAoQSlxuA=",
                                        "Pm6Y4ggcD1FhFAviA6EQSUtv/5qjT/IFIdrKNeCJtiyM=");

        assertEquals(so.userName, "test-user");
        assertEquals(so.userPrivateKey, "S0huLfhqH+046Hy9UYAY6r4N2/P7Hbeq94FyAoQSlxuA=");
        assertEquals(so.clusterPublicKey, "Pm6Y4ggcD1FhFAviA6EQSUtv/5qjT/IFIdrKNeCJtiyM=");
    }


    @Test
    public void canCreateSecurityOptionsOfFiles () throws IOException {
        Session.SecurityOptions so =
            Session.SecurityOptions.ofFiles("user_private.key",
                                            "cluster_public.key");

        assertEquals(so.userName, "test-user");
        assertEquals(so.userPrivateKey, "S0huLfhqH+046Hy9UYAY6r4N2/P7Hbeq94FyAoQSlxuA=");
        assertEquals(so.clusterPublicKey, "Pm6Y4ggcD1FhFAviA6EQSUtv/5qjT/IFIdrKNeCJtiyM=");
    }


    @Test
    public void canReadRelativePaths () throws IOException {
        Session.SecurityOptions so =
            Session.SecurityOptions.ofFiles("./user_private.key",
                                            "./cluster_public.key");

        assertEquals(so.userName, "test-user");
        assertEquals(so.userPrivateKey, "S0huLfhqH+046Hy9UYAY6r4N2/P7Hbeq94FyAoQSlxuA=");
        assertEquals(so.clusterPublicKey, "Pm6Y4ggcD1FhFAviA6EQSUtv/5qjT/IFIdrKNeCJtiyM=");
    }

    @Test
    public void canReadComplexPaths () throws IOException {
        Session.SecurityOptions so =
            Session.SecurityOptions.ofFiles("../qdb-api-jni/user_private.key",
                                            "../qdb-api-jni/cluster_public.key");

        assertEquals(so.userName, "test-user");
        assertEquals(so.userPrivateKey, "S0huLfhqH+046Hy9UYAY6r4N2/P7Hbeq94FyAoQSlxuA=");
        assertEquals(so.clusterPublicKey, "Pm6Y4ggcD1FhFAviA6EQSUtv/5qjT/IFIdrKNeCJtiyM=");
    }
}
