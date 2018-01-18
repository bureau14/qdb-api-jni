package net.quasardb.qdb;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.Clock;

import net.quasardb.qdb.jni.*;

public class QdbTimespec implements Serializable {
    private static Clock clock = new QdbClock();
    protected long sec;
    protected long nsec;

    public QdbTimespec(){
        this.sec = -1;
        this.nsec = -1;
    }

    public QdbTimespec(long sec, long nsec){
        this.sec = sec;
        this.nsec = nsec;
    }

    public QdbTimespec (LocalDateTime value) {
        this(value.atZone(ZoneId.systemDefault()).toEpochSecond(),
             value.getNano());
    }

    public QdbTimespec (Timestamp value) {
        this(value.toLocalDateTime());
    }

    public QdbTimespec (Instant value) {
        this(value.getEpochSecond(),
             value.getNano());
    }

    public long getSec() {
        return this.sec;
    }

    public long getNano() {
        return this.nsec;
    }

    public static QdbTimespec now() {
        return new QdbTimespec(Instant.now(QdbTimespec.clock));
    }

    public static QdbTimespec now(Clock clock) {
        return new QdbTimespec(Instant.now(clock));
    }

    /**
     * Returns copy of this instance with the specified duration in seconds added.
     */
    public QdbTimespec plusSeconds(long secondsToAdd) {
        return new QdbTimespec(this.sec + secondsToAdd,
                               this.nsec);
    }

    /**
     * Returns copy of this instance with the specified duration in nanoseconds added.
     */
    public QdbTimespec plusNanos(long nanosToAdd) {
        return new QdbTimespec(this.sec,
                               this.nsec + nanosToAdd);
    }

    public Instant asInstant() {
        return Instant.ofEpochSecond(this.sec,
                                     this.nsec);
    }

    public LocalDateTime asLocalDateTime() {
        return LocalDateTime.ofInstant(this.asInstant(),
                                       ZoneId.systemDefault());
    }

    public Timestamp asTimestamp() {
        return Timestamp.valueOf(this.asLocalDateTime());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbTimespec)) return false;
        QdbTimespec rhs = (QdbTimespec)obj;


        return rhs.getSec() == this.sec && rhs.getNano() == this.nsec;
    }

    public String toString() {
        return "QdbTimespec (sec: " + this.sec + ", nsec: " + this.nsec + ")";
    }
}
