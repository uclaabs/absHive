package org.apache.hadoop.hive.ql.abm.datatypes;

public class ContinuousSrvReader extends SrvReader {


  public ContinuousSrvReader(int numCols) {
    super(numCols);
  }

  public int getNumCondition(int bufLen) {
    return bufLen/(numCols * 2);
  }

  @Override
  public void setCondition(int condIdx) {
    offset = condIdx * numCols * 2;
  }

  @Override
  public double getMean(double[] buf, int colIdx) {
    return buf[offset + colIdx * 2];
  }

  @Override
  public double getVariance(double[] buf, int colIdx) {
    return buf[offset + colIdx * 2 + 1];
  }



}