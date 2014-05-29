package org.apache.hadoop.hive.ql.abm.udaf;

public class CaseCountComputation extends SrvCountComputation {

  public void setCount(int cnt) {
    this.baseCnt = cnt;
  }
  
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
