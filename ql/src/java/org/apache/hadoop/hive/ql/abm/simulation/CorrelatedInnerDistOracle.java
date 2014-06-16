package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray2D;
import org.apache.hadoop.hive.ql.abm.datatypes.PartialCovMap.InnerCovMap;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvReader;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class CorrelatedInnerDistOracle implements InnerDistOracle {

  private final Int2ReferenceOpenHashMap<double[]> srvs;
  private final SrvReader reader;
  private final int length;
  private final InnerCovMap inner;
  private final CovOracle[][] oracles;

  public CorrelatedInnerDistOracle(Int2ReferenceOpenHashMap<double[]> srvs,
      SrvReader reader, InnerCovMap inner, List<UdafType> aggrTypes) {
    this.srvs = srvs;
    this.reader = reader;
    length = reader.getNumCols();
    this.inner = inner;
    oracles = CovOracle.getCovOracles(aggrTypes, aggrTypes);
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
    reader.locate(srvs.get(groupId), condId);

    reader.fillMean(mean, offset);
    boolean fake = reader.fillVar(cov, offset);
    if (!fake) {
      DoubleArray2D pcov = inner.get(groupId);
      pcov.fill(condId, cov, offset);

      for (int i = 0; i < length; ++i) {
        for (int j = i + 1; j < length; ++j) {
          oracles[i][j].fillCovariance(mean, mean, offset, offset, cov);
        }
      }

      for (int i = offset, to = offset + length; i < to; ++i) {
        for (int j = i + 1; j < to; ++j) {
          cov[j][i] = cov[i][j];
        }
      }
    } // otherwise do nothing because the covariance matrix is already initialized to 0

    return fake;
  }

  @Override
  public void fillCov(int groupId, int condId1, int condId2, double[] mean, double[][] cov, int offset1, int offset2) {
    for (int i = 0; i < length; ++i) {
      for (int j = 0; j < length; ++j) {
        oracles[i][j].fillCovariance(mean, mean, offset1, offset2, cov);
      }
    }

    for (int i = offset1, ito = offset1 + length; i < ito; ++i) {
      for (int j = offset2, jto = offset2 + length; j < jto; ++j) {
        cov[j][i] = cov[i][j];
      }
    }
  }

}
