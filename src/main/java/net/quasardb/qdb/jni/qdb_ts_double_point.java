package net.quasardb.qdb.jni;

import net.quasardb.qdb.ts.Timespec;

public final class qdb_ts_double_point {
  protected Timespec timestamp;
  protected double value;

  public qdb_ts_double_point(){
    this.value = -1.0;
  }

  public qdb_ts_double_point(Timespec timestamp, double value){
    this.timestamp = timestamp;
    this.value = value;
  }

  public Timespec getTimestamp() {
    return this.timestamp;
  }

  public double getValue() {
      return this.value;
  }
}
