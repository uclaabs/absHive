package org.apache.hadoop.hive.ql.abm.simulation;

import java.io.Serializable;

public class MCSimNode implements Serializable {

  private static final long serialVersionUID = 1L;

  private int[] gbySizes;
  private MCSimNode child;

  public MCSimNode() {
  }

  public MCSimNode(int[] gbySzs, MCSimNode chld, boolean needCov) {
    gbySizes = gbySzs;
    child = chld;
  }

  public void init() {
    //
  }

  public int[] getGbySizes() {
    return gbySizes;
  }

  public void setGbySizes(int[] gbySizes) {
    this.gbySizes = gbySizes;
  }

  public MCSimNode getChild() {
    return child;
  }

  public void setChild(MCSimNode child) {
    this.child = child;
  }

}
