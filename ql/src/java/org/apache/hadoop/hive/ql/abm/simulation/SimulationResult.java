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

import java.util.ArrayList;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;

public class SimulationResult {

  public ArrayList<IntArrayList> lastCondId = null; // filled by dispatch

  public final ArrayList<double[]> means = new ArrayList<double[]>();
  public final ArrayList<ArrayList<IntArrayList>> condIds = new ArrayList<ArrayList<IntArrayList>>();
  public ArrayList<double[][]> samples = new ArrayList<double[][]>();
  public Array2DRowRealMatrix invSigma;

  @Override
  public SimulationResult clone() {
    SimulationResult ret = new SimulationResult();
    ret.means.addAll(means);
    ret.condIds.addAll(condIds);
    ret.invSigma = invSigma;
    return ret;
  }

  public double[] getSample(int idx) {
    double[] res = new double[invSigma.getColumnDimension()];
    int pos = 0;
    for(double[] smpl : samples.get(idx)) {
      if (smpl == null) {
        continue;
      }
      System.arraycopy(smpl, 0, res, pos, smpl.length);
      pos += smpl.length;
    }
    return res;
  }

  public double[] getMean() {
    double[] res = new double[invSigma.getColumnDimension()];
    int pos = 0;
    for (double[] mean : means) {
      System.arraycopy(mean, 0, res, pos, mean.length);
      pos += mean.length;
    }
    return res;
  }

}
