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

package org.apache.hadoop.hive.ql.abm.datatypes;

public abstract class SrvReader {

  public static final double FAKE_ZERO = 1000000;

  protected int numCols;
  protected double[] srv = null;
  protected int offset = 0;

  public SrvReader(int numCols) {
    this.numCols = numCols;
  }

  public int getNumCols() {
    return numCols;
  }

  public abstract void locate(double[] srv, int condId);

  public void fillMean(double[] dest, int pos) {
    System.arraycopy(srv, offset, dest, pos, numCols);
  }

  public abstract void fillVar(boolean[] fake, double[][] dest, int pos);

  public static SrvReader createReader(int numCols, boolean continuous) {
    if (continuous) {
      return new ContinuousSrvReader(numCols);
    } else {
      return new DiscreteSrvReader(numCols);
    }
  }

}
