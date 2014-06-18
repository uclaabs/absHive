package org.apache.hadoop.hive.ql.abm.simulation;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;


public abstract class CovOracle {

  public static int NUM_TUTPLES = 0;

  protected final int index1;
  protected final int index2;

  public CovOracle(int index1, int index2) {
    this.index1 = index1;
    this.index2 = index2;
  }

  public abstract void fillCovariance(double[] mean1, double[] mean2, int offset1,
      int offset2, double[][] partialCov);

  public static CovOracle[][] getCovOracles(UdafType[] aggrs1, UdafType[] aggrs2) {
    CovOracle[][] oracles = new CovOracle[aggrs1.length][aggrs2.length];

    for (int i = 0; i < aggrs1.length; ++i) {
      int type = (aggrs1[i] == UdafType.AVG) ? 2 : 0;
      for (int j = 0; j < aggrs2.length; ++j) {
        switch (type + ((aggrs2[j] == UdafType.AVG) ? 1 : 0)) {
        case 0:
          oracles[i][j] = new SumSumCovOracle(i, j);
          break;
        case 1:
          oracles[i][j] = new SumAvgCovOracle(i, j, aggrs2.length);
          break;
        case 2:
          oracles[i][j] = new AvgSumCovOracle(i, j, aggrs1.length);
          break;
        default: // case 3:
          oracles[i][j] = new AvgAvgCovOracle(i, j, aggrs1.length, aggrs2.length);
          break;
        }
      }
    }

    return oracles;
  }

}

class SumSumCovOracle extends CovOracle {

  public SumSumCovOracle(int index1, int index2) {
    super(index1, index2);
  }

  @Override
  public void fillCovariance(double[] mean1, double[] mean2, int offset1, int offset2,
      double[][] partialCov) {
    int i = offset1 + index1;
    int j = offset2 + index2;
    partialCov[i][j] = partialCov[i][j] - mean1[i] * mean2[j] / NUM_TUTPLES;
  }

}

class SumAvgCovOracle extends CovOracle {

  private final int last2;

  public SumAvgCovOracle(int index1, int index2, int length2) {
    super(index1, index2);
    last2 = length2 - 1;
  }

  @Override
  public void fillCovariance(double[] mean1, double[] mean2, int offset1, int offset2,
      double[][] partialCov) {
    int i = offset1 + index1;
    int j = offset2 + index2;
    int k = offset2 + last2;
    partialCov[i][j] = (partialCov[i][j] - mean2[j] * partialCov[i][k]) / mean2[k];
  }

}

class AvgSumCovOracle extends CovOracle {

  private final int last1;

  public AvgSumCovOracle(int index1, int index2, int length1) {
    super(index1, index2);
    last1 = length1 - 1;
  }

  @Override
  public void fillCovariance(double[] mean1, double[] mean2, int offset1, int offset2,
      double[][] partialCov) {
    int i = offset1 + index1;
    int j = offset1 + last1;
    int k = offset2 + index2;
    partialCov[i][k] = (partialCov[i][k] - mean1[i] * partialCov[j][k]) / mean1[j];
  }

}

class AvgAvgCovOracle extends CovOracle {

  private final int last1;
  private final int last2;

  public AvgAvgCovOracle(int index1, int index2, int length1, int length2) {
    super(index1, index2);
    last1 = length1 - 1;
    last2 = length2 - 1;
  }

  @Override
  public void fillCovariance(double[] mean1, double[] mean2, int offset1, int offset2,
      double[][] partialCov) {
    int i = offset1 + index1;
    int j = offset1 + last1;
    int k = offset2 + index2;
    int m = offset2 + last2;
    partialCov[i][k] = (partialCov[i][k] - mean1[i] * partialCov[j][k] - mean2[k] * partialCov[i][m]
        + mean1[i] * mean2[k] * partialCov[j][m]) / (mean1[j] * mean2[m]);
  }

}
