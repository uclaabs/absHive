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

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;

public class ConditionComputation extends UDAFComputation {

  private final List<Boolean> flags;
  private int cnt = -1;
  private final int dim;
  private final double[] newCondRanges;

  private List<RangeList> rangeMatrix = null;
//  private CondGroup condGroups = new CondGroup();
  private final List<CondList> condGroups = new ArrayList<CondList>();

  private final CondList condList = new CondList();

  public ConditionComputation(List<Boolean> flags) {
    this.flags = flags;
    dim = flags.size();
    newCondRanges = new double[dim];
  }

  public void setFields(IntArrayList keyArray, List<RangeList> rangeMatrix) {
    this.cnt ++;
    this.rangeMatrix = rangeMatrix;

    List<RangeList> emptyRangeMatrix = new ArrayList<RangeList>();
    for (int i = 0; i < dim; i++) {
      emptyRangeMatrix.add(new RangeList());
    }
    this.condGroups.add(new CondList(keyArray, emptyRangeMatrix));
  }

  public void clear() {
    condGroups.clear();
    condList.clear();
    rangeMatrix = null;
    cnt = -1;
  }

  @Override
  public void iterate(int index) {
  }

  @Override
  public void partialTerminate(int level, int index) {
    RangeList rangeList = rangeMatrix.get(level);
    newCondRanges[level] = rangeList.get(index);
  }

  @Override
  public void terminate() {
    List<RangeList> matrix = condGroups.get(cnt).getRangeMatrix();
    for (int i = 0; i < dim; i++) {
      matrix.get(i).add(newCondRanges[i]);
    }
  }

  @Override
  public void unfold() {
    if (this.dim == 0 || this.cnt + 1 == 0) {
      return;
    }

    // unfold the conditions
    for (CondList cond: condGroups) {
      condList.addKeys(cond.getKey());
    }

    for (int i = 0; i < dim * (this.cnt + 1); i++) {
      condList.addRanges(new RangeList());
    }

    double[] rangeArray = new double[dim * (this.cnt + 1)];
    unfoldRangeMatrix(0, rangeArray);
  }

  private void unfoldRangeMatrix(int level, double[] rangeArray) {
    boolean leaf = (level == cnt);
    int offset = level * dim;

    List<RangeList> currentRangeMatrix = condGroups.get(level).getRangeMatrix();
    int rowNumber = currentRangeMatrix.get(0).size();

    for (int j = 0; j < dim; ++j) {
      rangeArray[offset + j] = (flags.get(j)) ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    }

    if (leaf) {
      condList.addRange(rangeArray);
    } else {
      unfoldRangeMatrix(level + 1, rangeArray);
    }

    for (int i = 0; i < rowNumber; ++i) {
      for (int j = 0; j < dim; ++j) {
        rangeArray[offset + j] = currentRangeMatrix.get(j).get(i);
      }

      if (leaf) {
        condList.addRange(rangeArray);
      } else {
        unfoldRangeMatrix(level + 1, rangeArray);
      }
    }
  }

  @Override
  public Object serializeResult() {
    return condList.toArray();
  }

  @Override
  public void reset() {
  }

}
