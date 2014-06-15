package org.apache.hadoop.hive.ql.abm.datatypes;

public class DiscreteSrvReader extends SrvReader {

  public DiscreteSrvReader(int numCols) {
    super(numCols);
  }

  public int getNumCondition(int bufLen) {
    return bufLen/numCols;
  }

  @Override
  public void setCondition(int condIdx) {
    offset = condIdx * numCols;
  }

  @Override
  public double getMean(double[] buf, int colIdx) {
    return buf[offset + colIdx];
  }

  @Override
  public double getVariance(double[] buf, int colIdx) {
    return buf[offset + colIdx + 1];
  }

}