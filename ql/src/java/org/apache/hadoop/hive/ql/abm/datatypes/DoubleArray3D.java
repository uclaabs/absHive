package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.Serializable;

public class DoubleArray3D implements Serializable {

  private static final long serialVersionUID = 1L;

  private final double[] buf;
  private final int area;
  private final int len;
  private final int dim1;
  private final int dim2;

  public DoubleArray3D(int numRows1, int numRows2, int unfoldLen, int dim1, int dim2) {
    area = numRows2 * unfoldLen;
    len = unfoldLen;
    buf = new double[numRows1 * area];
    this.dim1 = dim1;
    this.dim2 = dim2;
  }

  public void fill(int idx1, int idx2, double[][] dest, int offset1, int offset2) {
    int pos = idx1 * area + idx2 * len;
    for (int i = offset1, to = offset1 + dim1; i < to; ++i) {
      System.arraycopy(buf, pos, dest[i], offset2, dim2);
      pos += dim2;
    }
  }

  public void updateRow(int idx1, int idx2, double[] vals1, double[] vals2) {
    int pos = idx1 * area + idx2 * len;
    for (int i = 0; i < vals1.length; ++i) {
      for (int j = 0; j < vals2.length; ++j) {
        buf[pos++] += vals1[i] * vals2[j];
      }
    }
  }

  public void merge(DoubleArray3D input) {
    for (int i = 0; i < buf.length; ++i) {
      buf[i] += input.buf[i];
    }
  }

  public void updateByBase() {
    int numRows1 = buf.length / area - 1;
    int numRows2 = area / len - 1;
    int baseOffset = numRows1 * area;

    // add the x base surface to the above
    int pos = 0;
    for (int i = 0; i < numRows1; ++i) {
      for (int j = baseOffset; j < buf.length; ++j) {
        buf[pos++] = buf[j];
      }
    }

    // add the y base to the left
    ++numRows1;
    int from, to = 0;
    pos = 0;
    for (int i = 0; i < numRows1; ++i) {
      // move to the base in this row
      to += area;
      from = to - len;
      for (int j = 0; j < numRows2; ++j) {
        for (int k = from; k < to; ++k) {
          buf[pos++] = buf[k];
        }
      }
      // skip the base in this row
      pos += len;
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append('{');
    int numRow1 = buf.length / area;
    int numRow2 = area / len;
    int pos = 0;
    for (int i = 0; i < numRow1; ++i) {
      builder.append('[');
      boolean firstRow = true;
      for (int j = 0; j < numRow2; ++j) {
        if (!firstRow) {
          builder.append("; ");
        }
        firstRow = false;

        boolean firstCol = true;
        for (int k = 0; k < len; k++) {
          if (!firstCol) {
            builder.append(", ");
          }
          firstCol = false;
          builder.append(buf[pos++]);
        }
      }
      builder.append(']');
    }
    builder.append('}');

    return builder.toString();
  }

}