package org.apache.hadoop.hive.ql.abm.datatypes;

public abstract class SrvReader {

  protected int numCols;
  protected int offset;

  public SrvReader(int numCols) {
    this.numCols = numCols;
    this.offset = 0;
  }

  public int getLength() {
    return numCols;
  }

  public abstract void setCondition(int condIdx) ;

  public abstract double getMean(double[] buf, int colIdx);

  public abstract double getVariance(double[] buf, int colIdx);

}
