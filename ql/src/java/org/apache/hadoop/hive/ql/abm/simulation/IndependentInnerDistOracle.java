package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvReader;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInnerDistOracle implements InnerDistOracle {

  private final Int2ReferenceOpenHashMap<SrvTuple> srvs;
  private final SrvReader reader;
  private final int length;

  public IndependentInnerDistOracle(Int2ReferenceOpenHashMap<SrvTuple> srvs, SrvReader reader, List<UdafType> udafTypes) {
    this.srvs = srvs;
    this.reader = reader;
    length = reader.getNumCols();
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
  public boolean fillMeanAndCov(int groupId, int condId, double[] mean, double[][] cov, int offset) {
    reader.locate(srvs.get(groupId).srv, condId);

    reader.fillMean(mean, offset);
    return reader.fillVar(cov, offset); // the covariance matrix is already initialized to 0
  }

  @Override
  public void fillCov(int groupId, int condId1, int condId2, double[] mean, double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

}
