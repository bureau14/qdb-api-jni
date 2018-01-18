package net.quasardb.qdb.jni;

import net.quasardb.qdb.ts.Timespec;
import java.nio.ByteBuffer;

public final class qdb_ts_blob_point {
  protected Timespec timestamp;
  protected ByteBuffer value;

  public qdb_ts_blob_point(Timespec timestamp, ByteBuffer value){
    if(!value.isDirect()) {
      throw new IllegalArgumentException("Not a direct ByteBuffer: " + value.toString());
    }
    this.timestamp = timestamp;
    this.value = value;
  }

  public Timespec getTimestamp() {
    return this.timestamp;
  }

  public ByteBuffer getValue() {
      return this.value;
  }
}
