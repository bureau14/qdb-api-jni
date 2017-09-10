package net.quasardb.qdb.jni;

public final class qdb_ts_double_point {
  protected qdb_timespec timestamp;
  protected double value;

  public qdb_ts_double_point(qdb_timespec timestamp, double value){
    this.timestamp = timestamp;
    this.value = value;
  }

  public qdb_timespec getTimestamp() {
    return this.timestamp;
  }

  public double getValue() {
      return this.value;
  }
}
