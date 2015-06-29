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

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray2D;
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType;
import org.apache.hadoop.hive.ql.abm.simulation.PartialCovMap.InnerCovMap;

public class CorrelatedInnerDistOracle extends InnerDistOracle {

  private final InnerCovMap inner;
  private final CovOracle[][] oracles;

  public CorrelatedInnerDistOracle(TupleMap srv, boolean continuous,
      IntArrayList groupIds, InnerCovMap inner, UdafType[] aggrTypes, Offset offInfo) {
    super(srv, continuous, groupIds, aggrTypes.length, offInfo);
    this.inner = inner;
    oracles = CovOracle.getCovOracles(aggrTypes, aggrTypes);
  }

  @Override
  protected void fillMeanAndCov(int groupId, int condId, boolean[] fake, double[] mean, double[][] cov, int offset) {
    reader.locate(srv.get(groupId).srv, condId);

    reader.fillMean(mean, offset);
    reader.fillVar(fake, cov, offset);

    if (!fake[offset]) {
      DoubleArray2D pcov = inner.get(groupId);
      pcov.fill(condId, cov, offset);

      for (int i = 0; i < elemDim; ++i) {
        for (int j = i + 1; j < elemDim; ++j) {
          oracles[i][j].fillCovariance(mean, mean, offset, offset, cov);
        }
      }

      for (int i = offset, to = offset + elemDim; i < to; ++i) {
        for (int j = i + 1; j < to; ++j) {
          cov[j][i] = cov[i][j];
        }
      }
    } // otherwise do nothing because the covariance matrix is already initialized to 0
  }

  @Override
  protected void fillCov(double[] mean, double[][] cov, int offset1, int offset2) {
    for (int i = 0; i < elemDim; ++i) {
      for (int j = 0; j < elemDim; ++j) {
        oracles[i][j].fillCovariance(mean, mean, offset1, offset2, cov);
      }
    }

    for (int i = offset1, ito = offset1 + elemDim; i < ito; ++i) {
      for (int j = offset2, jto = offset2 + elemDim; j < jto; ++j) {
        cov[j][i] = cov[i][j];
      }
    }
  }

}
