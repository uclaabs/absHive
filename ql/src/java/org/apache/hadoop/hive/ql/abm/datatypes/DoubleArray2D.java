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

  public void fill(int idx, double[][] dest, int offset) {
    int pos = idx * len;
    int len = dim - 1;
    for (int i = offset, to = dim + offset; i < to; ++i) {
      System.arraycopy(buf, pos, dest[i], i + 1, len);
      pos += len;
      --len;
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
    int numRows = buf.length / len;
    int pos = len;
    for (int i = 1; i < numRows; ++i) {
      for (int j = 0; j < len; ++j) {
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