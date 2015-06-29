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

package org.apache.hadoop.hive.ql.abm.udf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapperParser;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeMatrixParser;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ConditionJoin extends GenericUDF {

  private final IntArrayList inputKeys = new IntArrayList();
  private final List<RangeList> inputRanges = new ArrayList<RangeList>();
  private final Object[] ret = new Object[] {inputKeys, inputRanges};
  private boolean first = true;

  private StructObjectInspector[] inputOIs = null;
  private StructField[] keyFields = null;
  private StructField[] rangeFields = null;
  private KeyWrapperParser[] keyParsers = null;
  private RangeMatrixParser[] rangeParsers = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("This function takes at least two arguments of type CondGroup");
    }

    inputOIs = new StructObjectInspector[arguments.length];
    keyFields = new StructField[arguments.length];
    rangeFields = new StructField[arguments.length];
    keyParsers = new KeyWrapperParser[arguments.length];
    rangeParsers = new RangeMatrixParser[arguments.length];
    for(int i = 0; i < arguments.length; ++ i) {
      StructObjectInspector input = (StructObjectInspector) arguments[i];
      List<? extends StructField> fields = input.getAllStructFieldRefs();
      StructField key = fields.get(0);
      StructField range = fields.get(1);
      inputOIs[i] = input;
      keyFields[i] = key;
      rangeFields[i] = range;
      keyParsers[i] = new KeyWrapperParser(key.getFieldObjectInspector());
      rangeParsers[i] = new RangeMatrixParser(range.getFieldObjectInspector());
    }

    return CondList.condListOI;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    StringBuilder builder = new StringBuilder();
    builder.append("Cond_Join(");
    boolean first = true;
    for (String arg : arg0) {
      if (!first) {
        builder.append(", ");
      }
      first = false;
      builder.append(arg);
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    if (first) {
      for(int i = 0; i < arg.length; ++ i) {
        Object condGroupObj = arg[i].get();
        keyParsers[i].parseInto(inputOIs[i].getStructFieldData(condGroupObj, keyFields[i]), inputKeys);
        rangeParsers[i].append(inputOIs[i].getStructFieldData(condGroupObj, rangeFields[i]), inputRanges);
      }
      first = false;
    }

    inputKeys.clear();

    int cursor = 0;
    for(int i = 0; i < arg.length; ++ i) {
      Object condGroupObj = arg[i].get();
      keyParsers[i].parseInto(inputOIs[i].getStructFieldData(condGroupObj, keyFields[i]), inputKeys);
      cursor += rangeParsers[i].overwrite(inputOIs[i].getStructFieldData(condGroupObj, rangeFields[i]), inputRanges, cursor);
    }

    return ret;
  }

}