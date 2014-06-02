package org.apache.hadoop.hive.ql.abm.udaf;

public class CaseCountComputation extends SrvCountComputation {

  public void setCount(long cnt) {
    this.baseCnt = cnt;
  }

  @Override
  protected void addDistribution(double cnt) {
    this.result.add(cnt);

    if(cnt < this.confidenceLower) {
      this.confidenceLower = cnt;
    }
    if(cnt > this.confidenceUpper) {
      this.confidenceUpper = cnt;
    }
  }

}
