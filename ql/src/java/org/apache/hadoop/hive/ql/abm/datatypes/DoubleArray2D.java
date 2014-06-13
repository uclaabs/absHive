package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.Serializable;

public class DoubleArray2D implements Serializable {

  private static final long serialVersionUID = 1L;

  private final double[] buf;
  private final int rowLen;
  private final int len;

  public DoubleArray2D(int numRows, int unfoldLen, int len) {
    buf = new double[numRows * unfoldLen];
    rowLen = unfoldLen;
    this.len = len;
  }

  public void fill(int rowIndex, double[][] dest, int row, int col) {
    int offset = rowIndex * rowLen;
    for (int i = 0; i < len; ++i) {
      double[] cur = dest[i + row];
      for (int j = i + 1 + col, end = len + col; j < end; ++j) {
        cur[j] = buf[offset++];
      }
    }
  }

  public void updateRow(int rowIndex, double[] vals) {
    int offset = rowIndex * rowLen;
    for (int i = 0; i < vals.length; i++) {
      for (int j = i + 1; j < vals.length; j++) {
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
    int rows2Update = buf.length / rowLen - 1;
    int baseOffset = rows2Update * rowLen;

    int rowOffset = 0;
    for (int i = 0; i < rows2Update; i++) {
      for (int j = 0; j < rowLen; j++) {
        buf[rowOffset + j] += buf[baseOffset + j];
      }
      rowOffset += rowLen;
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append('[');
    int numRows = buf.length / rowLen;
    boolean firstRow = true;
    for (int i = 0; i < numRows; ++i) {
      if (!firstRow) {
        builder.append("; ");
      }
      firstRow = false;
      int rowOffset = rowLen * i;
      boolean first = true;
      for (int j = 0; j < rowLen; ++j) {
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
