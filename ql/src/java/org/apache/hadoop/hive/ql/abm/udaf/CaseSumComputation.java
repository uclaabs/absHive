package org.apache.hadoop.hive.ql.abm.udaf;

public class CaseSumComputation extends SrvSumComputation {

  public void setBase(double sum) {
    this.baseSum = sum;
  }

  @Override
  public void iterate(int index) {
    currentSum += this.currentList.getDouble(index);
  }

  @Override
  public void terminate() {
    doubleMatrix.get(groupCnt).add(currentSum);
  }

  @Override
  public void unfold() {

    if(groupCnt >= 0) {
      unfoldSrvList(0, this.baseSum);
    }

    addDistribution(this.baseSum);

    this.result.add(0, this.confidenceLower);
    this.result.add(1, this.confidenceUpper);
  }

  protected void unfoldSrvList(int level, double sum) {

    boolean leaf = (level == this.groupCnt);
    for(int i = 0; i < this.doubleMatrix.get(level).size(); i ++) {

      double tmpSum = sum + this.doubleMatrix.get(level).getDouble(i);
      if(leaf) {
        addDistribution(tmpSum);
      } else {
        unfoldSrvList(level + 1, tmpSum);
      }
    }
  }

  protected void addDistribution(double sum) {
    this.result.add(sum);
    if(sum < this.confidenceLower) {
      this.confidenceLower = sum;
    }
    if(sum > this.confidenceUpper) {
      this.confidenceUpper = sum;
    }
  }

}
