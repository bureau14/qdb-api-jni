package net.quasardb.qdb.jni;

public final class qdb_cluster_security_options {

  public String user_name;
  public String user_private_key;
  public String cluster_public_key;

  public qdb_cluster_security_options(String user_name,
                                      String user_private_key,
                                      String cluster_public_key) {
    System.out.println("1 user_name = " + user_name);
    System.out.println("1 user_private_key = " + user_private_key);
    System.out.println("1 cluster_public_key = " + cluster_public_key);
    this.user_name = user_name;
    this.user_private_key = user_private_key;
    this.cluster_public_key = cluster_public_key;
  }

}
