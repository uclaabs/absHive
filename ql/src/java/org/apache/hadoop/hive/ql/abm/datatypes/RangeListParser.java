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

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class RangeListParser {

  private ListObjectInspector oi = null;
  private DoubleObjectInspector eoi = null;

  public RangeListParser(ObjectInspector oi) {
    this.oi = (ListObjectInspector) oi;
    eoi = (DoubleObjectInspector) this.oi.getListElementObjectInspector();
  }

  public RangeList parse(Object o) {
    int length = oi.getListLength(o);
    RangeList ret = new RangeList(length);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
    return ret;
  }

  public void parseInto(Object o, RangeList ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
  }

  public boolean isBase(Object o) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      double e = eoi.get(oi.getListElement(o, i));
      if (e != Double.NEGATIVE_INFINITY && e != Double.POSITIVE_INFINITY) {
        return false;
      }
    }
    return true;
  }

}
