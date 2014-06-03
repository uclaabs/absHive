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
  
  public void updateRow(int rowIndex, int numCols, double[] vals) {
    int offset = getOffset(rowIndex);
    for(int i = 0; i < numCols; i ++) {
      for(int j = i + 1; j < numCols; j ++) {
        updateBy(offset, vals[i] * vals[j]);
        offset += 1;
      }
    }
  }
  
  public double get(int i) {
    return buf[i];
  }
  
  public void merge(DoubleArray2D input) {
    for(int i = 0; i < buf.length; i ++) {
      buf[i] += input.get(i);
    }
  }
  
  public int getNumRow() {
    return buf.length/cols;
  }
  
  public void updateByBase() {
    int rowNum = getNumRow();
    
    int baseRow = rowNum - 1;
    int baseOffset = getOffset(baseRow);
    
    for(int i = 0; i < rowNum - 1; i ++) {
      int rowOffset = getOffset(i);
      for(int j = 0; j < cols; j ++) {
        updateBy(rowOffset + j, get(baseOffset + j));
      }
    }
  }
  

}
