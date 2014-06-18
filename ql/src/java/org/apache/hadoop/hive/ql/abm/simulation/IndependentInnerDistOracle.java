package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInnerDistOracle extends InnerDistOracle {

  public IndependentInnerDistOracle(Int2ReferenceOpenHashMap<SrvTuple> srvs, boolean continuous,
      IntArrayList groupIds, List<UdafType> udafTypes, OffsetInfo offInfo) {
    super(srvs, continuous, groupIds, udafTypes.size(), offInfo);
  }

  @Override
  protected boolean fillMeanAndCov(int groupId, int condId, double[] mean, double[][] cov, int offset) {
    reader.locate(srv.get(groupId).srv, condId);

    reader.fillMean(mean, offset);
    return reader.fillVar(cov, offset); // the covariance matrix is already initialized to 0
  }

  @Override
  protected void fillCov(double[] mean, double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

}
