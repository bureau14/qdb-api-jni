package net.quasardb.qdb.jni;

import net.quasardb.qdb.ts.Timespec;

public final class qdb_ts_string_point {
  protected Timespec timestamp;
  protected String value;

  public qdb_ts_string_point(Timespec timestamp, String value){
    this.timestamp = timestamp;
    this.value = value;
  }

  public Timespec getTimestamp() {
    return this.timestamp;
  }

  public String getValue() {
    return this.value;
  }
}
