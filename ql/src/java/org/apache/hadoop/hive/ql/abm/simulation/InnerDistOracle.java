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

import org.apache.hadoop.hive.ql.abm.datatypes.SrvReader;

public abstract class InnerDistOracle {

  protected final TupleMap srv;
  protected final SrvReader reader;

  private final IntArrayList groupIds;
  protected final int elemDim;
  private final Offset offset;

  public InnerDistOracle(TupleMap srv, boolean continuous,
      IntArrayList groupIds, int elemDim, Offset offInfo) {
    this.srv = srv;
    reader = SrvReader.createReader(elemDim, continuous);
    this.groupIds = groupIds;
    this.elemDim = elemDim;
    this.offset = offInfo;
  }

  public void fill(IntArrayList condIds, boolean[] fake, double[] mean, double[][] cov) {
    for (int i = 0, off1 = offset.offset; i < groupIds.size(); ++i, off1 += elemDim) {
      fillMeanAndCov(groupIds.getInt(i), condIds.getInt(i), fake, mean, cov, off1);
      // if (!fake[off1]) {
      for (int j = i + 1, off2 = off1 + elemDim; j < groupIds.size(); ++j, off2 += elemDim) {
        fillCov(mean, cov, off1, off2);
      }
      // }
    }
  }

  protected abstract void fillMeanAndCov(int groupId, int condId, boolean[] fake, double[] mean,
      double[][] cov,
      int offset);

  protected abstract void fillCov(double[] mean, double[][] cov, int offset1, int offset2);

}
