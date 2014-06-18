package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray3D;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InterCovMap;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInterDistOracle extends InterDistOracle {

  private final InterCovMap inter;
  private final CovOracle[][] oracles;

  public CorrelatedInterDistOracle(IntArrayList groupIds1, IntArrayList groupIds2,
      InterCovMap inter, UdafType[] udafTypes1, UdafType[] udafTypes2,
      OffsetInfo offInfo1, OffsetInfo offInfo2) {
    super(groupIds1, groupIds2, udafTypes1.length, udafTypes2.length, offInfo1, offInfo2);
    this.inter = inter;
    oracles = CovOracle.getCovOracles(udafTypes1, udafTypes2);
  }

  @Override
  public void fillCovSym(int groupId1, int groupId2, int condId1, int condId2, double[] mean,
      double[][] cov, int offset1, int offset2) {
    DoubleArray3D pcov = inter.get(groupId1, groupId2);
    pcov.fill(condId1, condId2, cov, offset1, offset2);

    for (int i = 0; i < elemDim1; ++i) {
      for (int j = 0; j < elemDim2; ++j) {
        oracles[i][j].fillCovariance(mean, mean, offset1, offset2, cov);
      }
    }

    int ito = offset1 + elemDim1;
    int jto = offset2 + elemDim2;
    for (int i = offset1; i < ito; ++i) {
      for (int j = offset2; j < jto; ++j) {
        cov[j][i] = cov[i][j];
      }
    }
  }

  @Override
  public void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2, double[] mean1,
      double[] mean2, double[][] cov, int offset1, int offset2) {
    DoubleArray3D pcov = inter.get(groupId1, groupId2);
    pcov.fill(condId1, condId2, cov, offset1, offset2);

    for (int i = 0; i < elemDim1; ++i) {
      for (int j = 0; j < elemDim2; ++j) {
        oracles[i][j].fillCovariance(mean1, mean2, offset1, offset2, cov);
      }
    }
  }

}
