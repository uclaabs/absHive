/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  protected int groupCnt = -1;

  protected double currentSum = 0;
  protected double currentSsum = 0;
  protected int currentCnt = 0;

  protected double confidenceLower = Double.POSITIVE_INFINITY;
  protected double confidenceUpper = Double.NEGATIVE_INFINITY;

  public void setBase(double sum, double ssum, int cnt) {
    baseSum = sum;
    baseSsum = ssum;
    baseCnt = cnt;
  }

  public void setCurrentList(DoubleArrayList list) {
    doubleMatrix.add(new DoubleArrayList());
    currentList = list;
    currentSum = 0;
    currentSsum = 0;
    currentCnt = 0;
    groupCnt++;
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
    double value = currentList.getDouble(index);
    currentSum += value;
    currentSsum += (value * value);
    currentCnt += 1;
  }

  @Override
  public void partialTerminate(int level, int index) {
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
    double variance = ((ssum / cnt) - mu * mu) / cnt;
    double std = Math.sqrt(variance);

    result.add(mu);
    result.add(variance);

    double lower = mu - 3 * std;
    double upper = mu + 3 * std;

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
    result.add(0);

    if (groupCnt >= 0) {
      unfoldSrvList(0, baseSum, baseSsum, baseCnt);
    } else {
      addDistribution(baseSum, baseSsum, baseCnt);
    }

    result.set(0, confidenceLower);
    result.set(1, confidenceUpper);
  }

  protected void unfoldSrvList(int level, double sum, double ssum, double cnt) {
    boolean leaf = (level == groupCnt);

    if (leaf) {
      addDistribution(sum, ssum, cnt);
    } else {
      unfoldSrvList(level + 1, sum, ssum, cnt);
    }

    DoubleArrayList lev = doubleMatrix.get(level);
    for (int i = 0; i < lev.size();) {
      double tmpSum = sum + lev.getDouble(i++);
      double tmpSsum = ssum + lev.getDouble(i++);
      double tmpCnt = cnt + lev.getDouble(i++);

      if (leaf) {
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
