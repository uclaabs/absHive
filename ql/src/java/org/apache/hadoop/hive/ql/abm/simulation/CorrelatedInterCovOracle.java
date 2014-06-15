package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInterCovOracle implements InterCovOracle {

  private static final long serialVersionUID = 1L;

  private final int numRows;
  private final int numCols;

  public CorrelatedInterCovOracle(List<UdafType> udafTypes1, List<UdafType> udafTypes2) {
    numRows = udafTypes1.size();
    numCols = udafTypes2.size();
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
  public void fillCovMatrix(int groupId1, int groupId2, int condId1, int condId2, double[][] dest, int row, int col) {
    // TODO Auto-generated method stub

  }

}
