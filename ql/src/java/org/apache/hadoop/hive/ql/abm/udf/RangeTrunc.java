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

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class RangeTrunc extends GenericUDF {
  private final static ListObjectInspector retOi = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

  private ListObjectInspector loi = null;
  private DoubleObjectInspector eoi = null;

  private final DoubleWritable lower = new DoubleWritable(0);
  private final DoubleWritable upper = new DoubleWritable(0);
  private final ArrayList<Object> ret = new ArrayList<Object>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("This function takes 1 arguments of type Srv");
    }

    loi = (ListObjectInspector) arguments[0];
    eoi = (DoubleObjectInspector) loi.getListElementObjectInspector();

    ret.add(lower);
    ret.add(upper);
    return retOi;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o = arguments[0].get();
    lower.set(eoi.get(loi.getListElement(o, 0)));
    upper.set(eoi.get(loi.getListElement(o, 1)));
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder builder = new StringBuilder();
    builder.append("Range(");
    boolean first = true;
    for (String arg : children) {
      if (!first) {
        builder.append(", ");
      }
      first = false;
      builder.append(arg);
    }
    builder.append(")");
    return builder.toString();
  }

}
