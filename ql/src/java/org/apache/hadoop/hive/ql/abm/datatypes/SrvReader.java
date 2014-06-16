package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.Serializable;

public abstract class SrvReader implements Serializable {

  private static final long serialVersionUID = 1L;

  protected int numCols;
  protected transient double[] srv = null;
  protected transient int offset = 0;

  public SrvReader(int numCols) {
    this.numCols = numCols;
  }

  public int getNumCols() {
    return numCols;
  }

  public abstract void locate(double[] srv, int condId);

  public void fillMean(double[] dest, int pos) {
    System.arraycopy(srv, offset, dest, pos, numCols);
  }

  public abstract boolean fillVar(double[][] dest, int pos);

}
