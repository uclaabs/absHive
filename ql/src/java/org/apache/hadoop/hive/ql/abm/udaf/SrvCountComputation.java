package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.ArrayList;
import java.util.List;

public class SrvCountComputation extends UDAFComputation {

  protected List<LongArrayList> cntMatrix = new ArrayList<LongArrayList>();
  protected DoubleArrayList result = new DoubleArrayList();
  protected int N = 0;
  protected long baseCnt = 0;
  protected long currentCnt = 0;
  protected int groupCnt = -1;
  protected double confidenceLower = Double.POSITIVE_INFINITY;
  protected double confidenceUpper = Double.NEGATIVE_INFINITY;

  public void setTotalTupleNumber(int N) {
    this.N = N;
  }

  public void setCount(long base) {
    baseCnt = base;
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
    cntMatrix.add(new LongArrayList());
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

  protected void addDistribution(long cnt) {
    double variance = cnt * (1 - cnt * 1.0 / N);
    double std = Math.sqrt(variance);

    result.add(cnt);
    result.add(variance);

    double lower = cnt - 3 * std;
    double upper = cnt + 3 * std;

    if (lower < confidenceLower) {
      confidenceLower = lower;
    }
    if (upper > confidenceUpper) {
      confidenceUpper = upper;
    }
  }

  @Override
  public void unfold() {
    result.add(0);
    result.add(1);

    if (groupCnt >= 0) {
      unfoldSrvList(0, baseCnt);
    }

    result.set(0, confidenceLower);
    result.set(1, confidenceUpper);
  }

  protected void unfoldSrvList(int level, long cnt) {
    boolean leaf = (level == groupCnt);

    if (leaf) {
      addDistribution(cnt);
    } else {
      unfoldSrvList(level + 1, cnt);
    }

    LongArrayList lev = cntMatrix.get(level);
    for (int i = 0; i < lev.size();) {
      long tmpCnt = cnt + lev.getLong(i++);

      if (leaf) {
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
