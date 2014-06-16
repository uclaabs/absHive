package org.apache.hadoop.hive.ql.abm.simulation;


public interface DistOracle {

  public int getRowSize();

  public int getColSize();

}

interface InnerDistOracle extends DistOracle {

  public boolean fillMeanAndCov(int groupId, int condId, double[] mean, double[][] cov, int offset);

  public void fillCov(int groupId, int condId1, int condId2, double[] mean, double[][] cov,
      int offset1, int offset2);

}

interface InterDistOracle extends DistOracle {

  public void fillCovSym(int groupId1, int groupId2, int condId1, int condId2, double[] mean1,
      double[] mean2, double[][] cov, int offset1, int offset2);

  public void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2, double[] mean1,
      double[] mean2, double[][] cov, int offset1, int offset2);

}
