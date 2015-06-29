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

public class IndependentInnerDistOracle extends InnerDistOracle {

  public IndependentInnerDistOracle(TupleMap srv, boolean continuous,
      IntArrayList groupIds, UdafType[] udafTypes, Offset offInfo) {
    super(srv, continuous, groupIds, udafTypes.length, offInfo);
  }

  @Override
  protected void fillMeanAndCov(int groupId, int condId, boolean[] fake, double[] mean, double[][] cov, int offset) {
    reader.locate(srv.get(groupId).srv, condId);

    reader.fillMean(mean, offset);
    reader.fillVar(fake, cov, offset); // the covariance matrix is already initialized to 0
  }

  @Override
  protected void fillCov(double[] mean, double[][] cov, int offset1, int offset2) {
    // Do nothing, as cov should be initialized with all zeros
  }

}
