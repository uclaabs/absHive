package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class CaseAvgComputation extends SrvAvgComputation {

  public void setBase(double sum, int cnt) {
    baseSum = sum;
    baseCnt = cnt;
  }

  @Override
  public void iterate(int index) {
    double value = currentList.getDouble(index);
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
    result.add(mu);

    if (mu < confidenceLower) {
      confidenceLower = mu;
    }
    if (mu > confidenceUpper) {
      confidenceUpper = mu;
    }
  }

  @Override
  public void unfold() {
    result.add(0);
    result.add(0);

    if (groupCnt >= 0) {
      unfoldSrvList(0, baseSum, baseCnt);
    } else {
      addDistribution(baseSum, baseCnt);
    }

    result.set(0, confidenceLower);
    result.set(1, confidenceUpper);
  }

  protected void unfoldSrvList(int level, double sum, double cnt) {
    boolean leaf = (level == groupCnt);

    if (leaf) {
      addDistribution(sum, cnt);
    } else {
      unfoldSrvList(level + 1, sum, cnt);
    }

    DoubleArrayList lev = doubleMatrix.get(level);
    for (int i = 0; i < lev.size();) {
      double tmpSum = sum + lev.getDouble(i++);
      double tmpCnt = cnt + lev.getDouble(i++);

      if (leaf) {
        addDistribution(tmpSum, tmpCnt);
      } else {
        unfoldSrvList(level + 1, tmpSum, tmpCnt);
      }
    }
  }

}
