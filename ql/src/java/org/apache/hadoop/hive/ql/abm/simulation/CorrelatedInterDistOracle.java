package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray3D;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InterCovMap;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInterDistOracle implements InterDistOracle {

  private final int length1;
  private final int length2;
  private final InterCovMap inter;

  public CorrelatedInterDistOracle(InterCovMap inter, List<UdafType> udafTypes1,
      List<UdafType> udafTypes2) {
    this.inter = inter;
    length1 = udafTypes1.size();
    length2 = udafTypes2.size();
    // TODO Auto-generated constructor stub
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
    // TODO Auto-generated method stub
    DoubleArray3D pcov = inter.get(groupId1, groupId2);
    pcov.fill(condId1, condId2, cov, offset1, offset2);
    // TODO

    int ito = offset1 + length1;
    int jto = offset2 + length2;
    for (int i = offset1; i < ito; ++i) {
      for (int j = offset2; j < jto; ++j) {
        cov[j][i] = cov[i][j];
      }
    }
  }

  @Override
  public void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2, double[] mean,
      double[][] cov, int offset1, int offset2) {
    // TODO Auto-generated method stub

  }

}
