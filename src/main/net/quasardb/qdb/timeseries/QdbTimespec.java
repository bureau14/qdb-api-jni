package net.quasardb.qdb;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;

import net.quasardb.qdb.jni.*;

public class QdbTimespec implements Serializable {

    private qdb_timespec value;

    public QdbTimespec (qdb_timespec value) {
        this.value = value;
    }

    public QdbTimespec (LocalDateTime value) {
        this(new qdb_timespec(value.atZone(ZoneId.systemDefault()).toEpochSecond(),
                              value.getNano()));
    }

    public QdbTimespec (Timestamp value) {
        this(value.toLocalDateTime());
    }

    public qdb_timespec getValue() {
        return this.value;
    }

    public LocalDateTime asLocalDateTime() {
        return LocalDateTime.ofInstant(this.asInstant(),
                                       ZoneId.systemDefault());
    }

    public Instant asInstant() {
        return Instant.ofEpochSecond(this.value.getEpochSecond(),
                                     this.value.getNano());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbTimespec)) return false;
        QdbTimespec rhs = (QdbTimespec)obj;

        return true;
    }

    public String toString() {
        return "QdbTimespec (value: " + this.value.toString() + ")";
    }
}
