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
