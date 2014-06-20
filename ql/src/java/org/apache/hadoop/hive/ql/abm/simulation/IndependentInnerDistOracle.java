package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInnerDistOracle extends InnerDistOracle {

  public IndependentInnerDistOracle(TupleMap srvs, boolean continuous,
      IntArrayList groupIds, UdafType[] udafTypes, Offset offInfo) {
    super(srvs, continuous, groupIds, udafTypes.length, offInfo);
  }

  @Override
  protected void fillMeanAndCov(int groupId, int condId, boolean[] fake, double[] mean, double[][] cov, int offset) {
    reader.locate(srv.get(groupId).srv, condId);

    reader.fillMean(mean, offset);
    reader.fillVar(fake, cov, offset); // the covariance matrix is already initialized to 0
  }

  @Override
  protected void fillCov(double[] mean, double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

}
