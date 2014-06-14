package org.apache.hadoop.hive.ql.abm.datatypes;

public class DiscreteSrvReader {
  
  private int numCols;
  private int offset;
  
  public DiscreteSrvReader(int numCols) {
    this.numCols = numCols;
    this.offset = 0;
  }
  
  public int getNumCondition(int bufLen) {
    return bufLen/numCols;
  }
  
  public void setCondition(int condIdx) {
    offset = condIdx * numCols;
  }
  
  public double getMean(double[] buf, int colIdx) {
    return buf[offset + colIdx];
  }
  
  public double getVariance(double[] buf, int colIdx) {
    return buf[offset + colIdx + 1];
  }

}
