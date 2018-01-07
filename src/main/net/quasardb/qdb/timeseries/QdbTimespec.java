package net.quasardb.qdb;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;

import net.quasardb.qdb.jni.*;

public class QdbTimespec implements Serializable {
    private long sec;
    private long nsec;

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

    public long getSec() {
        return this.sec;
    }

    public long getNano() {
        return this.nsec;
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
