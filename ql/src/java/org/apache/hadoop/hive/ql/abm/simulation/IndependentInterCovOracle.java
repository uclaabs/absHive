package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInterCovOracle implements InterCovOracle {

  private static final long serialVersionUID = 1L;

  private final int numRows;
  private final int numCols;

  public IndependentInterCovOracle(List<UdafType> lhsTypes, List<UdafType> rhsTypes) {
    numRows = lhsTypes.size();
    numCols = rhsTypes.size();
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
    // Do nothing, as dest should be initialized with all zeros
  }

}
