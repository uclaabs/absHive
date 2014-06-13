package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray2D;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.Srv;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInnerCovOracle implements InnerCovOracle {

  private static final long serialVersionUID = 1L;

  private final Int2ReferenceOpenHashMap<Srv[]> srv;
  private final InnerCovMap inner;
  private final int length;

  public CorrelatedInnerCovOracle(Int2ReferenceOpenHashMap<Srv[]> srv, InnerCovMap inner, List<UdafType> udafTypes) {
    this.srv = srv;
    this.inner = inner;
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
    // TODO Auto-generated method stub
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

    DoubleArray2D pcov = inner.get(groupId);
    pcov.fill(condId, dest, row, col);

    // TODO

    return true;
  }

}
