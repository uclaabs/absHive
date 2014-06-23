package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class CaseSumComputation extends SrvSumComputation {

  public void setBase(double sum) {
    baseSum = sum;
  }

  @Override
  public void iterate(int index) {
    currentSum += currentList.getDouble(index);
  }

  @Override
  public void terminate() {
    doubleMatrix.get(groupCnt).add(currentSum);
  }

  @Override
  public void unfold() {
    result.add(0);
    result.add(0);

    if (groupCnt >= 0) {
      unfoldSrvList(0, baseSum);
    }

    result.set(0, confidenceLower);
    result.set(1, confidenceUpper);
  }

  protected void unfoldSrvList(int level, double sum) {
    boolean leaf = (level == groupCnt);

    if (leaf) {
      addDistribution(sum);
    } else {
      unfoldSrvList(level + 1, sum);
    }

    DoubleArrayList lev = doubleMatrix.get(level);
    for (int i = 0; i < lev.size();) {
      double tmpSum = sum + lev.getDouble(i++);

      if (leaf) {
        addDistribution(tmpSum);
      } else {
        unfoldSrvList(level + 1, tmpSum);
      }
    }
  }

  protected void addDistribution(double sum) {
    result.add(sum);

    if (sum < confidenceLower) {
      confidenceLower = sum;
    }
    if (sum > confidenceUpper) {
      confidenceUpper = sum;
    }
  }

}
