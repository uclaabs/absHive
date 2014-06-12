package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class InterCovOracle extends CovOracle {

  private static final long serialVersionUID = 1L;

  public InterCovOracle(List<UdafType> lhsTypes, List<UdafType> rhsTypes) {
    super(lhsTypes.size(), rhsTypes.size());
    // TODO Auto-generated constructor stub
  }

  @Override
  public void fillCovMatrix(double[][] dest, int row, int col) {
    // TODO Auto-generated method stub

  }

}
