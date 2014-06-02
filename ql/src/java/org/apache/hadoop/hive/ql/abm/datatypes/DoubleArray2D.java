package org.apache.hadoop.hive.ql.abm.datatypes;


public class DoubleArray2D {

  private final double[] buf;
  private final int cols;

  public DoubleArray2D(int numRows, int numCols) {
    buf = new double[numRows * numCols];
    cols = numCols;
  }

  public int getOffset(int rowIndex) {
    return rowIndex * cols;
  }

  public void updateBy(int index, double delta) {
    buf[index] += delta;
  }

}
