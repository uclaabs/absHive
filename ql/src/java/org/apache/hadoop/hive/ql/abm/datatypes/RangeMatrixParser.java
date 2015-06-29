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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class RangeMatrixParser {

  private ListObjectInspector oi = null;
  private RangeListParser parser = null;

  public RangeMatrixParser(ObjectInspector oi) {
    this.oi = (ListObjectInspector) oi;
    parser = new RangeListParser(this.oi.getListElementObjectInspector());
  }

  public List<RangeList> parse(Object o) {
    int length = oi.getListLength(o);
    List<RangeList> ret = new ArrayList<RangeList>(length);
    for (int i = 0; i < length; ++i) {
      ret.add(parser.parse(oi.getListElement(o, i)));
    }
    return ret;
  }

  public void parseInto(Object o, List<RangeList> ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      parser.parseInto(oi.getListElement(o, i), ret.get(i));
    }
  }

  public void append(Object o, List<RangeList> ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      ret.add(parser.parse(oi.getListElement(o, i)));
    }
  }

  public int overwrite(Object o, List<RangeList> ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      RangeList target = ret.get(i);
      target.clear();
      parser.parseInto(oi.getListElement(o, i), target);
    }
    return length;
  }

  public int overwrite(Object o, List<RangeList> ret, int start) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      RangeList target = ret.get(i + start);
      target.clear();
      parser.parseInto(oi.getListElement(o, i), target);
    }
    return length;
  }

  public boolean isBase(Object o) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      if (!parser.isBase(oi.getListElement(o, i))) {
        return false;
      }
    }
    return true;
  }

}
