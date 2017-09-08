package net.quasardb.qdb.jni;

public final class qdb_ts_column_info {
  public String name;
  public int type;

  public qdb_ts_column_info(String name, int type){
    this.name = name;
    this.type = type;
  }
}
