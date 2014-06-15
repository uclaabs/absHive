package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray2D;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvReader;

public class CorrelatedInnerCovOracle implements InnerCovOracle {

  private static final long serialVersionUID = 1L;

  private final Int2ReferenceOpenHashMap<double[]> srvs;
  private final SrvReader reader;
  private final InnerCovMap inner;
  private final int length;

  public CorrelatedInnerCovOracle(Int2ReferenceOpenHashMap<double[]> srvs, SrvReader reader, InnerCovMap inner) {
    this.srvs = srvs;
    this.reader = reader;
    this.inner = inner;
    this.length = reader.getLength();
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
    // TODO
    double[] srv = srvs.get(groupId);
    reader.setCondition(condId);

    if(reader.getVariance(srv, 0) == 0) {
      for (int i = pos, end = pos + length; i < end; ++i) {
        dest[i][i] = 1;
      }
      return false;
    }

    for(int i = 0; i < length; ++ i) {
      int idx = i + pos;
      dest[idx][idx] = reader.getVariance(srv, i);
      mean[idx] = reader.getMean(srv, i);
    }

    DoubleArray2D pcov = inner.get(groupId);
    pcov.fill(condId, dest, pos, pos);

    return true;
  }

  @Override
  public void fillCovMatrix(int groupId, int condId1, int condId2, double[][] dest, int row, int col) {
    // TODO Auto-generated method stub

  }

}
