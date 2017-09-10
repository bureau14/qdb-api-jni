package net.quasardb.qdb.jni;

public final class qdb_ts_double_aggregation {
  protected qdb_ts_range range;
  protected long aggregation_type;

  protected long count;
  protected qdb_ts_double_point result;

  public qdb_ts_double_aggregation(qdb_ts_range range, long aggregation_type) {
    this.range = range;
    this.aggregation_type = aggregation_type;
  }

  public qdb_ts_double_aggregation(qdb_ts_range range, long aggregation_type, long count, qdb_ts_double_point result) {
    this.range = range;
    this.aggregation_type = aggregation_type;
    this.count = count;
    this.result = result;
  }

  public qdb_ts_range getRange() {
    return this.range;
  }

  public long getAggregationType() {
    return this.aggregation_type;
  }

  public long getCount() {
    return this.count;
  }

  public qdb_ts_double_point getResult() {
    return this.result;
  }
}
