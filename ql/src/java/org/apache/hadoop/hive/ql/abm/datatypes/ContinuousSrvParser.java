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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ContinuousSrvParser extends SrvParser {

  public ContinuousSrvParser(ObjectInspector oi, int from, int to) {
    super(oi, from, to);
  }

  public double[] parse(Object o) {
    for(int i = 0; i < fields.length; ++i) {
      os[i] = oi.getStructFieldData(o, fields[i]);
    }

    int len = lois[0].getListLength(os[0]);
    double[] srvs = new double[(len - 2) * fields.length];

    // read two double values from each list at a time
    int pos = 0;
    for(int i = 2; i < len; ++i) {
     for(int j = 0; j < fields.length; ++j) {
       srvs[pos++] = eois[j].get(lois[j].getListElement(os[j], i));
     }
    }

    return srvs;
  }

}