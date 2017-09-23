package net.quasardb.qdb.jni;

public final class qdb_ts_filtered_range {
  qdb_ts_range range;
  qdb_ts_filter filter;

  public qdb_ts_filtered_range(qdb_ts_range range, qdb_ts_filter filter){
    this.range = range;
    this.filter = filter;
  }

  public qdb_ts_range getRange() {
    return this.range;
  }

  public qdb_ts_filter getFilter() {
    return this.filter;
  }
}
