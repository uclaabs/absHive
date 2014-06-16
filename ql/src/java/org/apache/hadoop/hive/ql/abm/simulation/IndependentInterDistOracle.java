package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInterDistOracle implements InterDistOracle {

  private final int length1;
  private final int length2;

  public IndependentInterDistOracle(List<UdafType> lhsTypes, List<UdafType> rhsTypes) {
    length1 = lhsTypes.size();
    length2 = rhsTypes.size();
  }

  @Override
  public int getRowSize() {
    return length1;
  }

  @Override
  public int getColSize() {
    return length2;
  }

  @Override
  public void fillCovSym(int groupId1, int groupId2, int condId1, int condId2, double[] mean,
      double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

  @Override
  public void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2, double[] mean,
      double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

}
