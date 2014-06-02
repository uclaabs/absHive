package org.apache.hadoop.hive.ql.abm.datatypes;


public class DoubleArray3D {

  private final double[] buf;
  private final int dim1;
  private final int dim2;

  public DoubleArray3D(int num0, int num1, int num2) {
    buf = new double[num0 * num1 * num2];
    dim1 = num1 * num2;
    dim2 = num2;
  }

  public int getOffset(int index1, int index2) {
    return index1 * dim1 + index2 * dim2;
  }

  public void update(int index, double delta) {
    buf[index] += delta;
  }

}
