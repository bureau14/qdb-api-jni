package net.quasardb.qdb;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;

import net.quasardb.qdb.jni.*;

public class QdbTimespec implements Serializable {

    protected LocalDateTime value;

    public QdbTimespec (Timestamp value) {
        this(value.toLocalDateTime());
    }

    public QdbTimespec (LocalDateTime value) {
        this.value = value;
    }

    public LocalDateTime getValue() {
        return this.value;
    }

    public qdb_timespec toNative() {
        return new qdb_timespec(this.value.atZone(ZoneId.systemDefault()).toEpochSecond(),
                                this.value.getNano());
    }

    public static QdbTimespec fromNative(qdb_timespec input) {
        return new QdbTimespec(LocalDateTime.ofInstant(Instant.ofEpochSecond(input.getEpochSecond(),
                                                                             input.getNano()),
                                                       ZoneId.systemDefault()));
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
