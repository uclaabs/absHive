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
    } else {
      addDistribution(baseSum);
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
