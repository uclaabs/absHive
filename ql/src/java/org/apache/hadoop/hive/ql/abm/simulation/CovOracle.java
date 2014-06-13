package org.apache.hadoop.hive.ql.abm.simulation;

import java.io.Serializable;

public interface CovOracle extends Serializable {

  public int getRowSize();

  public int getColSize();

}

interface InnerCovOracle extends CovOracle {

  public boolean fillCovMatrix(int groupId, int condId, double[][] dest, int row, int col);

}

interface InterCovOracle extends CovOracle {

  public void fillCovMatrix(int leftId, int rightId, double[][] dest, int row, int col);

}