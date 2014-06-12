package org.apache.hadoop.hive.ql.abm.simulation;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class DummyInnerCovOracle extends CovOracle {

  private static final long serialVersionUID = 1L;

  public DummyInnerCovOracle(List<UdafType> udafTypes) {
    super(udafTypes.size(), udafTypes.size());
    // TODO Auto-generated constructor stub
  }

  @Override
  public void fillCovMatrix(double[][] dest, int row, int col) {
    // TODO Auto-generated method stub

  }

}
