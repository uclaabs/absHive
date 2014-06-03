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

  public void updateBy(int index, double delta) {
    buf[index] += delta;
  }
  
  public void updateRow(int rowIndex1, int rowIndex2, int numCol1, int numCol2, double[] vals1, double[] vals2) {
    
    int offset = getOffset(rowIndex1, rowIndex2);
    for(int i = 0; i < numCol1; i ++) {
      for(int j = 0; j < numCol2; j ++) {
        updateBy(offset, vals1[i] * vals2[j]);
        offset += 1;
      }
    }
  }
  
  public double get(int i) {
    return buf[i];
  }
  
  public void merge(DoubleArray3D input) {
    for(int i = 0; i < buf.length; i ++) {
      buf[i] += input.get(i);
    }
  }
  
  public void updateByRow(int row1, int row2) {
    for(int i = 0; i < dim2; i ++) {
      updateBy(row1 + i, get(row1 + i));
    }
  }
  
  public void updateByBase() {
    
    // first get numRow1 and numRow2, which is num0 and num1 in constructor
    int numRow1 = buf.length/dim1;
    int numRow2 = dim1/dim2;
    
    // both are base
    int baseOffset = getOffset(numRow1 - 1, numRow2 - 1);
    
    for(int i = 0; i < numRow1 - 1; i ++) {
      int baseOffset1 = getOffset(i, numRow2 - 1);
      for(int j = 0; j < numRow2 - 1; j ++) {
        int baseOffset2 = getOffset(numRow1 - 1, j);
        int rowOffset = getOffset(i, j);
        updateByRow(rowOffset, baseOffset);
        updateByRow(rowOffset, baseOffset1);
        updateByRow(rowOffset, baseOffset2);
      }
    } 
    
    for(int i = 0; i < numRow1 - 1; i ++) {
      int rowOffset = getOffset(i, numRow2 - 1);
      updateByRow(rowOffset, baseOffset);
    }
    
    for(int i = 0; i < numRow2 - 1; i ++) {
      int rowOffset = getOffset(numRow1 - 1, i);
      updateByRow(rowOffset, baseOffset);
    }
    
    //
    
  }

}
