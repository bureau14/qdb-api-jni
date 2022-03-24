package net.quasardb.qdb.jni;

import java.nio.ByteBuffer;

/**
 * Constant definitions, which reflects the constants that the QDB C API also uses.
 */
public class Constants {
    public static final double nullDouble = Double.NaN;
    public static final long nullInt64 = 0x8000000000000000L;
    public static final long minTime = 0x8000000000000000L;
    public static final long maxTime = 0xFFFFFFFFFFFFFFFFL;
    public static final long nullTime = minTime;
    public static final ByteBuffer nullBlob = null;
    public static final String nullString = null;
    public static final long nullPtr = 0;


    public static final int qdb_ts_column_uninitialized = -1;
    public static final int qdb_ts_column_double = 0;
    public static final int qdb_ts_column_blob = 1;
    public static final int qdb_ts_column_int64 = 2;
    public static final int qdb_ts_column_timestamp = 3;
    public static final int qdb_ts_column_string = 4;
    public static final int qdb_ts_column_symbol = 5;

};
