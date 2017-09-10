package net.quasardb.qdb.jni;

public final class qdb_ts_double_point {
  qdb_timespec timestamp;
  double value;

  public qdb_ts_double_point(qdb_timespec timestamp, double value){
    this.timestamp = timestamp;
    this.value = value;
  }
}
