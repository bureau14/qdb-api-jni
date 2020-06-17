package net.quasardb.qdb.jni;

/**
 * Constant definitions, which reflects the constants that the QDB C API also uses.
 */
public class Constants {
    public static double nullDouble = Double.NaN;
    public static long nullInt64 = 0x8000000000000000L;
    public static long minTime = 0x8000000000000000L;
    public static long maxTime = 0xFFFFFFFFFFFFFFFFL;
    public static long nullTime = minTime;

};
