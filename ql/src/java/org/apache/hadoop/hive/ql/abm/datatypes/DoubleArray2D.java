package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.Serializable;

public class DoubleArray2D implements Serializable {

  private static final long serialVersionUID = 1L;

  private final double[] buf;
  private final int len;
  private final int dim;

  public DoubleArray2D(int numRows, int unfoldLen, int dim) {
    buf = new double[numRows * unfoldLen];
    len = unfoldLen;
    this.dim = dim;
  }

  public void fill(int idx, double[][] dest, int row, int col) {
    int pos = idx * len;
    for (int i = 0; i < dim; ++i) {
      double[] cur = dest[i + row];
      for (int j = i + 1 + col, end = dim + col; j < end; ++j) {
        cur[j] = buf[pos++];
      }
    }
  }

  public void updateRow(int idx, double[] vals) {
    int offset = idx * len;
    for (int i = 0; i < vals.length; ++i) {
      for (int j = i + 1; j < vals.length; ++j) {
        buf[offset++] += vals[i] * vals[j];
      }
    }
  }

  public void merge(DoubleArray2D input) {
    for (int i = 0; i < buf.length; ++i) {
      buf[i] += input.buf[i];
    }
  }

  public void updateByBase() {
    int rows2Update = buf.length / len - 1;
    int baseOffset = rows2Update * len;

    int pos = 0;
    for (int i = 0; i < rows2Update; ++i) {
      for (int j = baseOffset; j < buf.length; ++j) {
        buf[pos++] += buf[j];
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append('[');
    int numRows = buf.length / len;
    int pos = 0;
    boolean firstRow = true;
    for (int i = 0; i < numRows; ++i) {
      if (!firstRow) {
        builder.append("; ");
      }
      firstRow = false;

      boolean firstCol = true;
      for (int j = 0; j < len; ++j) {
        if (!firstCol) {
          builder.append(", ");
        }
        firstCol = false;
        builder.append(buf[pos++]);
      }
    }
    builder.append(']');

    return builder.toString();
  }

}