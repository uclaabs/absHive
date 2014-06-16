package org.apache.hadoop.hive.ql.abm.udaf;

public class CaseCountComputation extends SrvCountComputation {

  @Override
  public void setCount(long cnt) {
    baseCnt = cnt;
  }

  @Override
  protected void addDistribution(long cnt) {
    result.add(cnt);

    if (cnt < confidenceLower) {
      confidenceLower = cnt;
    }
    if (cnt > confidenceUpper) {
      confidenceUpper = cnt;
    }
  }

}
