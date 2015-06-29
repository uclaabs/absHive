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


public class ContinuousSrvReader extends SrvReader {

  public ContinuousSrvReader(int numCols) {
    super(numCols);
  }

  public int getNumCondition(int bufLen) {
    return bufLen / (numCols * 2);
  }

  @Override
  public void locate(double[] srv, int condId) {
    this.srv = srv;
    offset = condId * numCols * 2;
  }

  @Override
  public void fillVar(boolean[] fake, double[][] dest, int pos) {
    for (int i = offset + numCols, to = i + numCols; i < to; ++i, ++pos) {
      double v = srv[i];
      if (v != 0) {
        dest[pos][pos] = v;
        fake[pos] = false;
      } else {
        dest[pos][pos] = FAKE_ZERO;
        fake[pos] = true;
      }
    }
  }

}
