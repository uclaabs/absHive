package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.List;

public class SrvSumComputation extends UDAFComputation {

  protected DoubleArrayList result = new DoubleArrayList();
  protected List<DoubleArrayList> doubleMatrix = new ArrayList<DoubleArrayList>();
  protected DoubleArrayList currentList = new DoubleArrayList();
  protected double baseSum = 0;
  protected double baseSsum = 0;
//  protected int N = AbmUtilities.getTotalTupleNumber();
  protected int N = 0;
  protected int groupCnt = -1;

  protected double currentSum = 0;
  protected double currentSsum = 0;
  protected double confidenceLower = Double.POSITIVE_INFINITY;
  protected double confidenceUpper = Double.NEGATIVE_INFINITY;

  public void setTotalTupleNumber(int N) {
    this.N = N;
  }

  public void setBase(double sum, double ssum) {
    this.baseSum = sum;
    this.baseSsum = ssum;
  }

  public void setCurrentList(DoubleArrayList list) {
    this.doubleMatrix.add(new DoubleArrayList());
    this.currentList = list;
    this.currentSum = 0;
    this.currentSsum = 0;
    this.groupCnt++;
  }

  public void clear() {
    result.clear();
    doubleMatrix.clear();
    currentList.clear();
    confidenceLower = Double.POSITIVE_INFINITY;
    confidenceUpper = Double.NEGATIVE_INFINITY;
    groupCnt = -1;
  }

  @Override
  public void iterate(int index) {
    double value = this.currentList.getDouble(index);
    currentSum += value;
    currentSsum += (value * value);
  }

  @Override
  public void partialTerminate(int level, int index) {
  }

  @Override
  public void terminate() {
    doubleMatrix.get(groupCnt).add(currentSum);
    doubleMatrix.get(groupCnt).add(currentSsum);
  }

  @Override
  public void reset() {
    currentSum = 0;
    currentSsum = 0;
  }

  @Override
  public void unfold() {
    if(groupCnt >= 0) {
      unfoldSrvList(0, this.baseSum, this.baseSsum);
    }

    result.add(0); // dummy place holder
    result.add(0); // dummy place holder
    addDistribution(this.baseSum, this.baseSsum);
    result.set(0, this.confidenceLower);
    result.set(1, this.confidenceUpper);
    // update the first two values of array
  }

  protected void addDistribution(double sum, double ssum) {
    double variance = ssum - sum * sum / N;
    double std = Math.sqrt(variance);

    result.add(sum);
    result.add(variance);

    double lower = sum - 3 * std;
    double upper = sum + 3 * std;

    if (lower < confidenceLower) {
      confidenceLower = lower;
    }
    if (upper > confidenceUpper) {
      confidenceUpper = upper;
    }
  }

  protected void unfoldSrvList(int level, double sum, double ssum) {
    boolean leaf = (level == groupCnt);
    DoubleArrayList lev = doubleMatrix.get(level);
    for (int i = 0; i < lev.size(); i += 2) {

      double tmpSum = sum + lev.getDouble(i);
      double tmpSsum = ssum + lev.getDouble(i + 1);

      if (leaf) {
        addDistribution(tmpSum, tmpSsum);
      } else {
        unfoldSrvList(level + 1, tmpSum, tmpSsum);
      }
    }
  }

  @Override
  public Object serializeResult() {
    return result;
  }

}
