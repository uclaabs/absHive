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

package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public abstract class InterDistOracle {

  private final IntArrayList groupIds1;
  private final IntArrayList groupIds2;
  protected final int elemDim1;
  protected final int elemDim2;
  private final Offset offInfo1;
  private final Offset offInfo2;

  public InterDistOracle(IntArrayList groupIds1, IntArrayList groupIds2, int elemDim1,
      int elemDim2, Offset offInfo1, Offset offInfo2) {
    this.groupIds1 = groupIds1;
    this.groupIds2 = groupIds2;
    this.elemDim1 = elemDim1;
    this.elemDim2 = elemDim2;
    this.offInfo1 = offInfo1;
    this.offInfo2 = offInfo2;
  }

  public void fillSym(IntArrayList condIds1, IntArrayList condIds2,
      boolean[] fake, double[] mean, double[][] cov) {
    for (int i = 0, off1 = offInfo1.offset; i < groupIds1.size(); ++i, off1 += elemDim1) {
      // if (!fake[off1]) {
      int groupId1 = groupIds1.getInt(i);
      int condId1 = condIds1.getInt(i);
      for (int j = 0, off2 = offInfo2.offset; j < groupIds2.size(); ++j, off2 += elemDim2) {
        // if (!fake[off2]) {
        fillCovSym(groupId1, groupIds2.getInt(j), condId1, condIds2.getInt(j),
            mean, cov, off1, off2);
        // }
      }
      // }
    }
  }

  public int fillAsym(IntArrayList condIds1, IntArrayList condIds2, boolean[] fake1,
      double[] mean1, double[] mean2, double[][] cov, int cum) {
    for (int i = 0, off1 = cum + offInfo1.offset; i < groupIds1.size(); ++i, off1 += elemDim1) {
      // if (!fake1[off1]) {
      int groupId1 = groupIds1.getInt(i);
      int condId1 = condIds1.getInt(i);
      for (int j = 0, off2 = offInfo2.offset; j < groupIds2.size(); ++j, off2 += elemDim2) {
        fillCovAsym(groupId1, groupIds2.getInt(j), condId1, condIds2.getInt(j),
            mean1, mean2, cov, off1, off2);
      }
      // }
    }
    return groupIds1.size() * elemDim1;
  }

  public abstract void fillCovSym(int groupId1, int groupId2, int condId1, int condId2,
      double[] mean, double[][] cov, int offset1, int offset2);

  public abstract void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2,
      double[] mean1, double[] mean2, double[][] cov, int offset1, int offset2);

}
