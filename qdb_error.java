package net.quasardb.qdb.jni;

public class qdb_error {
  public static final int ok = 0;
  public static final int uninitialized = 0xc300FFFF;
  public static final int alias_not_found = 0xb1000008;
  public static final int alias_already_exists = 0xb1000009;
  public static final int out_of_bounds = 0xc1000019;
  public static final int skipped = 0xb1000021;
  public static final int incompatible_type = 0xb1000022;
  public static final int container_empty = 0xb1000023;
  public static final int container_full = 0xb1000024;
  public static final int element_not_found = 0xb0000025;
  public static final int element_already_exists = 0xb0000026;
  public static final int overflow = 0xb1000027;
  public static final int underflow = 0xb1000028;
  public static final int tag_already_set = 0xb0000029;
  public static final int tag_not_set = 0xb000002a;
  public static final int timeout = 0xd200000a;
  public static final int connection_refused = 0xd300000e;
  public static final int connection_reset = 0xd200000f;
  public static final int unstable_cluster = 0xd2000012;
  public static final int try_again = 0xd2000017;
  public static final int conflict = 0xb200001a;
  public static final int not_connected = 0xd200001b;
  public static final int resource_locked = 0xb200002d;
  public static final int system_remote = 0xf3000001;
  public static final int system_local = 0xe3000001;
  public static final int internal_remote = 0xf3000002;
  public static final int internal_local = 0xe3000002;
  public static final int no_memory_remote = 0xf3000003;
  public static final int no_memory_local = 0xe3000003;
  public static final int invalid_protocol = 0xa3000004;
  public static final int host_not_found = 0xd2000005;
  public static final int buffer_too_small = 0xc100000b;
  public static final int not_implemented = 0xf3000011;
  public static final int invalid_version = 0xa3000016;
  public static final int invalid_argument = 0xc2000018;
  public static final int invalid_handle = 0xc200001c;
  public static final int reserved_alias = 0xc200001d;
  public static final int unmatched_content = 0xb000001e;
  public static final int invalid_iterator = 0xc200001f;
  public static final int entry_too_large = 0xc200002b;
  public static final int transaction_partial_failure = 0xb200002c;
  public static final int operation_disabled = 0xb200002e;
  public static final int operation_not_permitted = 0xb200002f;
  public static final int iterator_end = 0xb0000030;
  public static final int invalid_reply = 0xa3000031;
  public static final int ok_created = 0xb0000032;

  public static int origin(int err) {
    return err & 0xf0000000;
  }

  public static int severity(int err) {
    return err & 0x0f000000;
  }
}
