package net.quasardb.qdb;

import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InputException;

public class SessionFactoryTest {

    @Test
    public void canCreateNewSession () {
        SessionFactory sf = new SessionFactory(TestUtils.CLUSTER_URI);
	Session s = sf.newSession();

        assertFalse(s.isClosed());
    }

    @Test
    public void canSetSecurityOptions () {
        Session.SecurityOptions so =
            new Session.SecurityOptions("test-user", "xxx", "xxx");

        SessionFactory sf = new SessionFactory(TestUtils.CLUSTER_URI)
	    .securityOptions(so);

	// bad credentials raise an InputException error.
	assertThrows(InputException.class, () -> {
		sf.newSession();
	    });
    }

    @Test
    public void canSetInputBufferSize () {
        long ibs = 8589934592L;
	SessionFactory sf = new SessionFactory(TestUtils.CLUSTER_URI)
	    .inputBufferSize(ibs);

	Session s = sf.newSession();

	assertEquals(s.getInputBufferSize(), ibs);
    }

    @Test
    public void canSetSoftMemoryLimit () {
        long sml = 8589934592L;
	SessionFactory sf = new SessionFactory(TestUtils.CLUSTER_URI)
	    .softMemoryLimit(sml);

	Session s = sf.newSession();
	String mem_stats = s.getMemoryInfo();

	Pattern pattern = Pattern.compile("TBB soft limit bytes = (\\d+)");
	Matcher matcher = pattern.matcher(mem_stats);

	assertTrue(matcher.find(), "TBB soft limit bytes not found");

	long actual = Long.parseLong(matcher.group(1));

	assertEquals(actual, sml);
    }
}
