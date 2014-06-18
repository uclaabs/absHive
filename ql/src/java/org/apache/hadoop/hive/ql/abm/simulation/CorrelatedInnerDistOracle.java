package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray2D;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInnerDistOracle extends InnerDistOracle {

  private final InnerCovMap inner;
  private final CovOracle[][] oracles;

  public CorrelatedInnerDistOracle(Int2ReferenceOpenHashMap<SrvTuple> srv, boolean continuous,
      IntArrayList groupIds, InnerCovMap inner, List<UdafType> aggrTypes, OffsetInfo offInfo) {
    super(srv, continuous, groupIds, aggrTypes.size(), offInfo);
    this.inner = inner;
    oracles = CovOracle.getCovOracles(aggrTypes, aggrTypes);
  }

  @Override
  protected boolean fillMeanAndCov(int groupId, int condId, double[] mean, double[][] cov, int offset) {
    reader.locate(srv.get(groupId).srv, condId);

    reader.fillMean(mean, offset);
    boolean fake = reader.fillVar(cov, offset);
    if (!fake) {
      DoubleArray2D pcov = inner.get(groupId);
      pcov.fill(condId, cov, offset);

      for (int i = 0; i < elemDim; ++i) {
        for (int j = i + 1; j < elemDim; ++j) {
          oracles[i][j].fillCovariance(mean, mean, offset, offset, cov);
        }
      }

      for (int i = offset, to = offset + elemDim; i < to; ++i) {
        for (int j = i + 1; j < to; ++j) {
          cov[j][i] = cov[i][j];
        }
      }
    } // otherwise do nothing because the covariance matrix is already initialized to 0

    return fake;
  }

  @Override
  protected void fillCov(double[] mean, double[][] cov, int offset1, int offset2) {
    for (int i = 0; i < elemDim; ++i) {
      for (int j = 0; j < elemDim; ++j) {
        oracles[i][j].fillCovariance(mean, mean, offset1, offset2, cov);
      }
    }

    for (int i = offset1, ito = offset1 + elemDim; i < ito; ++i) {
      for (int j = offset2, jto = offset2 + elemDim; j < jto; ++j) {
        cov[j][i] = cov[i][j];
      }
    }
  }

}
