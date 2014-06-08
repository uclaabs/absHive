package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.Serializable;

public class DoubleArray2D implements Serializable {

  private static final long serialVersionUID = 1L;

  private final double[] buf;
  private final int cols;

  public DoubleArray2D(int numRows, int numCols) {
    buf = new double[numRows * numCols];
    cols = numCols;
  }

  public void updateRow(int rowIndex, int numCols, double[] vals) {
    int offset = rowIndex * cols;
    for (int i = 0; i < numCols; i++) {
      for (int j = i + 1; j < numCols; j++) {
        buf[offset] += vals[i] * vals[j];
        offset += 1;
      }
    }
  }

  public void merge(DoubleArray2D input) {
    for (int i = 0; i < buf.length; i++) {
      buf[i] += input.buf[i];
    }
  }

  public void updateByBase() {
    int rowNum = buf.length / cols - 1;
    int baseOffset = rowNum * cols;

    int rowOffset = 0;
    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < cols; j++) {
        buf[rowOffset + j] += buf[baseOffset + j];
      }
      rowOffset += cols;
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append('[');
    int rowNum = buf.length / cols;
    boolean firstRow = true;
    for (int i = 0; i < rowNum; ++i) {
      if (!firstRow) {
        builder.append("; ");
      }
      firstRow = false;
      int rowOffset = cols * i;
      boolean first = true;
      for (int j = 0; j < cols; ++j) {
        if (!first) {
          builder.append(", ");
        }
        first = false;
        builder.append(buf[rowOffset + j]);
      }
    }
    builder.append(']');

    return builder.toString();
  }

}
