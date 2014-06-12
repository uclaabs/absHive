package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class DummyInterCovOracle extends CovOracle {

  private static final long serialVersionUID = 1L;

  public DummyInterCovOracle(List<UdafType> lhsTypes, List<UdafType> rhsTypes) {
    super(lhsTypes.size(), rhsTypes.size());
  }

  @Override
  public void fillCovMatrix(double[][] dest, int row, int col) {
    // Do nothing, as dest should be initialized with all zeros
  }

}
