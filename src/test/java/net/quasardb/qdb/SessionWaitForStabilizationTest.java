package net.quasardb.qdb;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;

import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.exception.InputBufferTooSmallException;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class SessionWaitForStabilizationTest {

    private static Session s;

    @BeforeAll
    public static void setup() {
        this.s = TestUtils.createSession();
    }

    @AfterAll
    public static void teardown() {
        s.close();
        s = null;
    }

    @Test
    public void canWaitForStabilization() {
        s.waitForStabilization(60000);
    }
}
