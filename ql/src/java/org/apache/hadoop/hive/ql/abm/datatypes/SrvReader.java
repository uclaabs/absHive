package org.apache.hadoop.hive.ql.abm.datatypes;


public abstract class SrvReader {

  protected int numCols;
  protected double[] srv = null;
  protected int offset = 0;

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

  public static SrvReader createReader(int numCols, boolean continuous) {
    if (continuous) {
      return new ContinuousSrvReader(numCols);
    } else {
      return new DiscreteSrvReader(numCols);
    }
  }

}
