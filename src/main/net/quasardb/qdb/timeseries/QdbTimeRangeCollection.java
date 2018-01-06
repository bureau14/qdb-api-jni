package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;
import java.util.ArrayList;

public class QdbTimeRangeCollection extends ArrayList<QdbTimeRange> {

    public qdb_ts_filtered_range[] toNative() {
        return this.stream()
            .map(QdbTimeRange::toNative)
            .toArray(qdb_ts_filtered_range[]::new);
    }
}
