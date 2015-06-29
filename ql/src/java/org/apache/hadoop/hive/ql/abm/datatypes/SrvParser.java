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

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class SrvParser {

  protected StructObjectInspector oi;
  protected StructField[] fields;
  protected ListObjectInspector[] lois;
  protected DoubleObjectInspector[] eois;
  protected Object[] os;

  public SrvParser(ObjectInspector oi, int from, int to) {
    this.oi = (StructObjectInspector) oi;
    List<? extends StructField> allFields = this.oi.getAllStructFieldRefs();
    int len = to - from;
    fields = new StructField[len];
    lois = new ListObjectInspector[len];
    eois = new DoubleObjectInspector[len];
    os = new Object[len];

    for (int i = 0; i < len; ++i) {
      fields[i] = allFields.get(i + from);
      lois[i] = (ListObjectInspector) fields[i].getFieldObjectInspector();
      eois[i] = (DoubleObjectInspector) lois[i].getListElementObjectInspector();
    }
  }

}