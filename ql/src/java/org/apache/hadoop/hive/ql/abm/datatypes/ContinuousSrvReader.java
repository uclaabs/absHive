package org.apache.hadoop.hive.ql.abm.datatypes;

public class ContinuousSrvReader {
  
  private int numCols;
  private int offset;
  
  public ContinuousSrvReader(int numCols) {
    this.numCols = numCols;
    this.offset = 0;
  }
  
  public int getNumCondition(int bufLen) {
    return bufLen/(numCols * 2);
  }
  
  public void setCondition(int condIdx) {
    offset = condIdx * numCols * 2;
  }
  
  public double getMean(double[] buf, int colIdx) {
    return buf[offset + colIdx * 2];
  }
  
  public double getVariance(double[] buf, int colIdx) {
    return buf[offset + colIdx * 2 + 1];
  }

}