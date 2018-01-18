package net.quasardb.qdb;

import java.io.Serializable;
import net.quasardb.qdb.jni.*;

public class QdbTimeRange implements Serializable {

    protected QdbTimespec begin;
    protected QdbTimespec end;

    public QdbTimeRange (QdbTimespec begin, QdbTimespec end) {
        this.begin = begin;
        this.end = end;
    }

    public QdbTimespec getBegin() {
        return this.begin;
    }

    public QdbTimespec getEnd() {
        return this.end;
    }

    public String toString() {
        return "QdbTimeRange (begin: " + this.begin.toString() + ", end: " + this.end.toString() + ")";
    }
}
