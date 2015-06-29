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

import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;

public class IndependentInterDistOracle extends InterDistOracle {

  public IndependentInterDistOracle(IntArrayList groupIds1, IntArrayList groupIds2,
      UdafType[] udafTypes1, UdafType[] udafTypes2, Offset offInfo1, Offset offInfo2) {
    super(groupIds1, groupIds2, udafTypes1.length, udafTypes2.length, offInfo1, offInfo2);
  }

  @Override
  public void fillCovSym(int groupId1, int groupId2, int condId1, int condId2, double[] mean,
      double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

  @Override
  public void fillCovAsym(int groupId1, int groupId2, int condId1, int condId2, double[] mean1,
      double[] mean2, double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

}
