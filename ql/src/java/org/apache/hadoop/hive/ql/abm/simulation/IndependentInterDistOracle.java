package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInterDistOracle extends InterDistOracle {

  public IndependentInterDistOracle(IntArrayList groupIds1, IntArrayList groupIds2,
      UdafType[] udafTypes1, UdafType[] udafTypes2, Offset offInfo1, Offset offInfo2) {
    super(groupIds1, groupIds2, udafTypes1.length, udafTypes2.length, offInfo1, offInfo2);
  }

  @Override
  public void fillCovSym(int groupId1, int groupId2, int condId1, int condId2, double[] mean,
      double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

  @Override
  public void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2, double[] mean1,
      double[] mean2, double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

}
