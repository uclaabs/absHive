package org.apache.hadoop.hive.ql.abm.rewrite;

public enum UdafType {
  SUM(0),
  COUNT(1),
  AVG(2);

  public final int index;

  private UdafType(int i) {
    index = i;
  }
}
