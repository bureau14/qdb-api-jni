import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.IncompatibleTypeException;

public class ValueTest {

    @Test
    public void canCreateInt64() throws Exception {
        long l = TestUtils.randomInt64();
        Value value = Value.createInt64(l);

        assertEquals(l, value.getInt64());
    }

    @Test
    public void canCreateDouble() throws Exception {
        double d = TestUtils.randomDouble();
        Value value = Value.createDouble(d);
        assertEquals(d, value.getDouble());
    }

    @Test
    public void canCreateTimestamp() throws Exception {
        Timespec t = TestUtils.randomTimestamp();
        Value value = Value.createTimestamp(t);

        assertEquals(t, value.getTimestamp());
    }

    @Test
    public void canCreateBlob() throws Exception {
        ByteBuffer b = TestUtils.createSampleData();
        Value value = Value.createBlob(b);

        assertEquals(b, value.getBlob());
    }

    @Test
    public void canCreateSafeBlob() throws Exception {
        Value value1 =
            Value.createSafeBlob(TestUtils.createSampleData());
        Value value2 =
            Value.createSafeBlob(value1.getBlob());

        assertEquals(value1, value2);
    }

    @Test
    public void throwsError_whenDoubleTypesDontMatch() throws Exception {
        ByteBuffer b = TestUtils.createSampleData();
        Value value = Value.createBlob(b);

        assertThrows(IncompatibleTypeException.class, () -> {
                value.getDouble();
            });
    }

    @Test
    public void throwsError_whenBlobTypesDontMatch() throws Exception {
        double d = TestUtils.randomDouble();
        Value value = Value.createDouble(d);

        assertThrows(IncompatibleTypeException.class, () -> {
                value.getBlob();
            });
    }

    @Test
    public void canCompareInt64s() throws Exception {
        Value value1 = Value.createInt64(TestUtils.randomInt64());
        Value value2 = Value.createInt64(TestUtils.randomInt64());
        Value value3 = Value.createInt64(value1.getInt64());

        assertNotEquals(value1, value2);
        assertEquals(value1, value3);
        assertNotEquals(value2, value3);
    }

    @Test
    public void canCompareDoubles() throws Exception {
        Value value1 = Value.createDouble(TestUtils.randomDouble());
        Value value2 = Value.createDouble(TestUtils.randomDouble());
        Value value3 = Value.createDouble(value1.getDouble());

        assertNotEquals(value1, value2);
        assertEquals(value1, value3);
        assertNotEquals(value2, value3);
    }

    @Test
    public void canCompareTimestamps() throws Exception {
        Value value1 = Value.createTimestamp(TestUtils.randomTimestamp());
        Value value2 = Value.createTimestamp(TestUtils.randomTimestamp());
        Value value3 = Value.createTimestamp(value1.getTimestamp());

        assertNotEquals(value1, value2);
        assertEquals(value1, value3);
        assertNotEquals(value2, value3);
    }

    @Test
    public void canCompareBlobs() throws Exception {
        ByteBuffer b1 = TestUtils.createSampleData();
        ByteBuffer b2 = TestUtils.createSampleData();
        Value value1 = Value.createBlob(b1);
        Value value2 = Value.createBlob(b2);
        Value value3 = Value.createBlob(b1);

        assertNotEquals(value1, value2);
        assertEquals(value1, value3);
        assertNotEquals(value2, value3);
    }

    @Test
    public void canCompareBlobAndDoubles() throws Exception {
        ByteBuffer b = TestUtils.createSampleData();
        Double d = TestUtils.randomDouble();
        Value value1 = Value.createBlob(b);
        Value value2 = Value.createDouble(d);

        assertNotEquals(value1, value2);
    }

    @Test
    public void canSerialize_andDeserialize_Int64s() throws Exception {
        Value vBefore = Value.createInt64(TestUtils.randomInt64());

        byte[] serialized = TestUtils.serialize(vBefore);
        Value vAfter = (Value)TestUtils.deserialize(serialized, vBefore.getClass());

        assertEquals(vBefore, vAfter);
    }

    @Test
    public void canSerialize_andDeserialize_Doubles() throws Exception {
        Value vBefore = Value.createDouble(TestUtils.randomDouble());

        byte[] serialized = TestUtils.serialize(vBefore);
        Value vAfter = (Value)TestUtils.deserialize(serialized, vBefore.getClass());

        assertEquals(vBefore, vAfter);
    }

    @Test
    public void canSerialize_andDeserialize_Timestamps() throws Exception {
        Value vBefore = Value.createTimestamp(TestUtils.randomTimestamp());

        byte[] serialized = TestUtils.serialize(vBefore);
        Value vAfter = (Value)TestUtils.deserialize(serialized, vBefore.getClass());

        assertEquals(vBefore, vAfter);
    }

    @Test
    public void canSerialize_andDeserialize_Blobs() throws Exception {
        Value vBefore = Value.createBlob(TestUtils.createSampleData());

        byte[] serialized = TestUtils.serialize(vBefore);
        Value vAfter = (Value)TestUtils.deserialize(serialized, vBefore.getClass());

        assertEquals(vBefore, vAfter);
    }
}
