package net.quasardb.qdb.jni;

import net.quasardb.qdb.QdbTimespec;

public final class qdb_ts_double_point {
  protected QdbTimespec timestamp;
  protected double value;

  public qdb_ts_double_point(){
    this.value = -1.0;
  }

  public qdb_ts_double_point(QdbTimespec timestamp, double value){
    this.timestamp = timestamp;
    this.value = value;
  }

  public QdbTimespec getTimestamp() {
    return this.timestamp;
  }

  public double getValue() {
      return this.value;
  }
}
