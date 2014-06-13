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
  public boolean fillCovMatrix(int groupId, int condId, double[][] dest, int row, int col) {
    Srv[] allCols = srv.get(groupId);
    if (allCols[0].getVar(condId) == 0) {
      for (int i = 0; i < length; ++i) {
        dest[i + row][i + col] = 1; // fill in a fake variance
      }
      return false;
    }

    for (int i = 0; i < length; ++i) {
      dest[i + row][i + col] = allCols[i].getVar(condId);
    }
    return true;
  }

}
