package org.apache.hadoop.hive.ql.abm.udaf;

public class CaseAvgComputation extends SrvAvgComputation {

  public void setBase(double sum, int cnt) {
    this.baseSum = sum;
    this.baseCnt = cnt;
  }

  @Override
  public void iterate(int index) {
    double value = this.currentList.getDouble(index);
    currentSum += value;
    currentCnt += 1;
  }

  @Override
  public void terminate() {
    doubleMatrix.get(groupCnt).add(currentSum);
    doubleMatrix.get(groupCnt).add(currentCnt);
  }

  protected void addDistribution(double sum, double cnt) {
    double mu = sum / cnt;
    this.result.add(mu);

    if(mu < this.confidenceLower) {
      this.confidenceLower = mu;
    }
    if(mu > this.confidenceUpper) {
      this.confidenceUpper = mu;
    }
  }

  @Override
  public void unfold() {

    if(groupCnt >= 0) {
      unfoldSrvList(0, this.baseSum, this.baseCnt);
    }

    addDistribution(this.baseSum, this.baseCnt);
    this.result.add(0, this.confidenceLower);
    this.result.add(1, this.confidenceUpper);
  }

  protected void unfoldSrvList(int level, double sum, double cnt) {
    boolean leaf = (level == this.groupCnt);
    for(int i = 0; i < this.doubleMatrix.get(level).size() / 2; i ++) {
      double tmpSum = sum + this.doubleMatrix.get(level).getDouble(i * 2);
      double tmpCnt = cnt + this.doubleMatrix.get(level).getDouble(i * 2 + 1);

      if(leaf) {
        addDistribution(tmpSum, tmpCnt);
      } else {
        unfoldSrvList(level + 1, tmpSum, tmpCnt);
      }
    }
  }

}
