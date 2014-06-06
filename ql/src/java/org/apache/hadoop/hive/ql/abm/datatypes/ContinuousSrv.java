package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class ContinuousSrv extends DoubleArrayList {

  private static final long serialVersionUID = 1L;

  public ContinuousSrv(int capacity) {
    super(capacity);
  }

  @Override
  public String toString() {
    //
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    for (int i = 0; i < size(); i += 2) {
      sb.append(this.get(i));
      sb.append(", ");
      sb.append(this.get(i+1));
      sb.append("; ");
    }
    sb.append(")");
    return sb.toString();
  }
}