package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;

public class SrvCountComputation extends UDAFComputation {

  protected List<IntArrayList> cntMatrix = new ArrayList<IntArrayList>();
  protected DoubleArrayList result = new DoubleArrayList();
  protected int N = AbmUtilities.getTotalTupleNumber();
  protected long baseCnt = 0;
  protected int currentCnt = 0;
  protected int groupCnt = -1;
  protected double confidenceLower = Double.POSITIVE_INFINITY;
  protected double confidenceUpper = Double.NEGATIVE_INFINITY;

  public void setCount (int base) {
    this.baseCnt = base;
  }

  public void clear() {
    result.clear();
    cntMatrix.clear();
    currentCnt = 0;
    confidenceLower = Double.POSITIVE_INFINITY;
    confidenceUpper = Double.NEGATIVE_INFINITY;
    groupCnt = -1;
  }

  public void addNewGroup() {
    cntMatrix.add(new IntArrayList());
    groupCnt += 1;
  }

  @Override
  public void iterate(int index) {
    currentCnt += 1;
  }

  @Override
  public void partialTerminate(int level, int index) {
  }

  @Override
  public void terminate() {
    cntMatrix.get(groupCnt).add(currentCnt);
  }

  @Override
  public void reset() {
    currentCnt = 0;
  }

  protected void addDistribution(double cnt) {
    double variance = cnt * (1 - cnt / this.N);
    double std = Math.sqrt(variance);

    this.result.add(cnt);
    this.result.add(variance);

    double lower = cnt - 3 * std;
    double upper = cnt + 3 * std;

    if(lower < this.confidenceLower) {
      this.confidenceLower = lower;
    }
    if(upper > this.confidenceUpper) {
      this.confidenceUpper = upper;
    }
  }

  @Override
  public void unfold() {

    unfoldSrvList(0, this.baseCnt);
    addDistribution(this.baseCnt);
    this.result.add(0, this.confidenceLower);
    this.result.add(1, this.confidenceUpper);
  }

  protected void unfoldSrvList(int level, long cnt) {

    boolean leaf = (level == this.groupCnt);
    for(int i = 0; i < this.cntMatrix.get(level).size(); i ++) {

      long tmpCnt = cnt + this.cntMatrix.get(level).getInt(i);

      if(leaf) {
        addDistribution(tmpCnt);
      } else {
        unfoldSrvList(level + 1, tmpCnt);
      }
    }
  }

  @Override
  public Object serializeResult() {
    return result;
  }

}
