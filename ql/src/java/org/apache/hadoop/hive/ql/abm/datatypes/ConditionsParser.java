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

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ConditionsParser {

  private final StructObjectInspector oi;
  private final StructField key;
  private final StructField ranges;
  private final KeyWrapperParser keyParser;
  private final RangeMatrixParser rangesParser;

  private IntArrayList keyOutput = null;
  private List<RangeList> rangeOutput = null;

  public ConditionsParser(ObjectInspector oi) {
    this.oi = (StructObjectInspector) oi;
    List<? extends StructField> fields = this.oi.getAllStructFieldRefs();
    key = fields.get(0);
    ranges = fields.get(1);
    keyParser = new KeyWrapperParser(key.getFieldObjectInspector());
    rangesParser = new RangeMatrixParser(ranges.getFieldObjectInspector());
  }

  public IntArrayList parseKey(Object o) {
    return keyParser.parse(oi.getStructFieldData(o, key));
  }

  public List<RangeList> parseRange(Object o) {
    return rangesParser.parse(oi.getStructFieldData(o, ranges));
  }

  public IntArrayList inplaceParseKey(Object o) {
    if (keyOutput == null) {
      keyOutput = keyParser.parse(oi.getStructFieldData(o, key));
    } else {
      keyOutput.clear();
      keyParser.parseInto(oi.getStructFieldData(o, key), keyOutput);
    }
    return keyOutput;
  }

  public List<RangeList> inplaceParseRange(Object o) {
    if (rangeOutput == null) {
      rangeOutput = rangesParser.parse(oi.getStructFieldData(o, ranges));
    } else {
      rangesParser.overwrite(oi.getStructFieldData(o, ranges), rangeOutput);
    }
    return rangeOutput;
  }

}
