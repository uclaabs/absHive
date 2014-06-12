package org.apache.hadoop.hive.ql.abm.simulation;

import java.io.Serializable;

public abstract class CovOracle implements Serializable {

  private static final long serialVersionUID = 1L;

  protected int rowSize;
  protected int colSize;

  public CovOracle(int rows, int cols) {
    rowSize = rows;
    colSize = cols;
  }

  public abstract void fillCovMatrix(double[][] dest, int row, int col);

  public int getRowSize() {
    return rowSize;
  }

  public int getColSize() {
    return colSize;
  }

}
