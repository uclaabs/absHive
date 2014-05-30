package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.List;

public class SrvAvgComputation extends UDAFComputation {

  protected DoubleArrayList result = new DoubleArrayList();
  protected List<DoubleArrayList> doubleMatrix = new ArrayList<DoubleArrayList>();
  protected DoubleArrayList currentList = new DoubleArrayList();
  protected double baseSum = 0;
  protected double baseSsum = 0;
  protected int baseCnt = 0;

  protected int N = 0;
  protected int groupCnt = -1;

  protected double currentSum = 0;
  protected double currentSsum = 0;
  protected int currentCnt = 0;

  protected double confidenceLower = Double.POSITIVE_INFINITY;
  protected double confidenceUpper = Double.NEGATIVE_INFINITY;

  public void setTotalNumber(int N) {
    this.N = N;
  }

  public void setBase(double sum, double ssum, int cnt) {
    this.baseSum = sum;
    this.baseSsum = ssum;
    this.baseCnt = cnt;
  }

  public void setCurrentList(DoubleArrayList list) {
    this.doubleMatrix.add(new DoubleArrayList());
    this.currentList = list;
    this.currentSum = 0;
    this.currentSsum = 0;
    this.currentCnt = 0;
    this.groupCnt ++;
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
    currentCnt += 1;
  }

  @Override
  public void partialTerminate(int level, int start, int end) {
  }

  @Override
  public void terminate() {
    doubleMatrix.get(groupCnt).add(currentSum);
    doubleMatrix.get(groupCnt).add(currentSsum);
    doubleMatrix.get(groupCnt).add(currentCnt);
  }

  @Override
  public void reset() {
    currentSum = 0;
    currentSsum = 0;
    currentCnt = 0;
  }

  protected void addDistribution(double sum, double ssum, double cnt) {
    double mu = sum / cnt;
    double variance = ((ssum/cnt) - mu * mu)/cnt;
    double std = Math.sqrt(variance);

    this.result.add(mu);
    this.result.add(variance);

    double lower = mu - 3 * std;
    double upper = mu + 3 * std;

    if(lower < this.confidenceLower) {
      this.confidenceLower = lower;
    }
    if(upper > this.confidenceUpper) {
      this.confidenceUpper = upper;
    }
  }

  @Override
  public void unfold() {
    unfoldSrvList(0, this.baseSum, this.baseSsum, this.baseCnt);

    addDistribution(this.baseSum,this.baseSsum, this.baseCnt);
    this.result.add(0, this.confidenceLower);
    this.result.add(1, this.confidenceUpper);
  }

  protected void unfoldSrvList(int level, double sum, double ssum, double cnt) {
    boolean leaf = (level == this.groupCnt);
    for(int i = 0; i < this.doubleMatrix.get(level).size() / 3; i ++) {

      double tmpSum = sum + this.doubleMatrix.get(level).getDouble(i * 3);
      double tmpSsum = ssum + this.doubleMatrix.get(level).getDouble(i * 3 + 1);
      double tmpCnt = cnt + this.doubleMatrix.get(level).getDouble(i * 3 + 2);

      if(leaf) {
        addDistribution(tmpSum, tmpSsum, tmpCnt);
      } else {
        unfoldSrvList(level + 1, tmpSum, tmpSsum, tmpCnt);
      }
    }
  }

  @Override
  public Object serializeResult() {
    return result;
  }

}
