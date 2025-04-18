package net.quasardb.qdb;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;

import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.exception.InputBufferTooSmallException;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class SessionWaitForStabilizationTest {

    private Session s;

    @BeforeEach
    public void setup() {
        s = TestUtils.createSession();
        s.purgeAll(30000);
    }

    @AfterEach
    public void teardown() {
        s.close();
        s = null;
    }

    @Test
    public void canWaitForStabilization() {
        s.waitForStabilization(60000);
    }
}
