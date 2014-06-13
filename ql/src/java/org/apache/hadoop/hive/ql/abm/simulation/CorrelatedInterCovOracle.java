package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInterCovOracle implements InterCovOracle {

  private static final long serialVersionUID = 1L;

  private final int numRows;
  private final int numCols;

  public CorrelatedInterCovOracle(List<UdafType> lhsTypes, List<UdafType> rhsTypes) {
    numRows = lhsTypes.size();
    numCols = rhsTypes.size();
    // TODO Auto-generated constructor stub
  }

  @Override
  public int getRowSize() {
    return numRows;
  }

  @Override
  public int getColSize() {
    return numCols;
  }

  @Override
  public void fillCovMatrix(int leftId, int rightId, double[][] dest, int row, int col) {
    // TODO Auto-generated method stub

  }

}
