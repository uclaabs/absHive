package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.Srv;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInnerCovOracle implements InnerCovOracle {

  private static final long serialVersionUID = 1L;

  private final Int2ReferenceOpenHashMap<Srv[]> srv;
  private final int length;

  public IndependentInnerCovOracle(Int2ReferenceOpenHashMap<Srv[]> srv, List<UdafType> udafTypes) {
    this.srv = srv;
    length = udafTypes.size();
  }

  @Override
  public int getRowSize() {
    return length;
  }

  @Override
  public int getColSize() {
    return length;
  }

  @Override
  public boolean fillCovMatrix(int groupId, int condId, double[][] dest, double[] mean, int pos) {
    Srv[] allCols = srv.get(groupId);
    if (allCols[0].getVar(condId) == 0) {
      for (int i = pos, end = pos + length; i < end; ++i) {
        dest[i][i] = 1; // fill in a fake variance
      }
      return false;
    }

    for (int i = 0; i < length; ++i) {
      int idx = i + pos;
      dest[idx][idx] = allCols[i].getVar(condId);
    }
    return true;
  }

  @Override
  public void fillCovMatrix(int groupId, int condId1, int condId2, double[][] dest, int row, int col) {
    // Do nothing, as dest should be initialized with all zeros
  }

}
