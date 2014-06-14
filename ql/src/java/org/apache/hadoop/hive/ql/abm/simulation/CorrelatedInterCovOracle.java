package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray3D;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InterCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.Srv;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInterCovOracle implements InterCovOracle {

  private static final long serialVersionUID = 1L;

  private final Int2ReferenceOpenHashMap<Srv[]> srv1;
  private final Int2ReferenceOpenHashMap<Srv[]> srv2;
  private final InterCovMap inter;
  private final int numRows;
  private final int numCols;

  public CorrelatedInterCovOracle(Int2ReferenceOpenHashMap<Srv[]> srv1, Int2ReferenceOpenHashMap<Srv[]> srv2,
      InterCovMap inter, List<UdafType> udafTypes1, List<UdafType> udafTypes2) {
    this.srv1 = srv1;
    this.srv2 = srv2;
    this.inter = inter;
    numRows = udafTypes1.size();
    numCols = udafTypes2.size();
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
    DoubleArray3D pcov = inter.get(groupId1, groupId2);
    pcov.fill(condId1, condId2, dest, row, col);
    // TODO
  }

}
