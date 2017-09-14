package net.quasardb.qdb.jni;

import java.nio.ByteBuffer;

public final class qdb_ts_blob_point {
  protected qdb_timespec timestamp;
  protected ByteBuffer value;

  public qdb_ts_blob_point(qdb_timespec timestamp, ByteBuffer value){
    this.timestamp = timestamp;
    this.value = value;
  }

  public qdb_timespec getTimestamp() {
    return this.timestamp;
  }

  public ByteBuffer getValue() {
      return this.value;
  }
}
