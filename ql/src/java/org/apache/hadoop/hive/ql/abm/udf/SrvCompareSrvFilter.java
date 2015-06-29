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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

public abstract class SrvCompareSrvFilter extends CompareUDF {

  protected BooleanWritable ret = new BooleanWritable(false);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes two arguments: Srv, Srv");
    }

    super.initialize(arguments);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    double lower1 = elemOI.get(srvOI.getListElement(arg[0].get(), 0));
    double upper1 = elemOI.get(srvOI.getListElement(arg[0].get(), 1));
    double lower2 = elemOI.get(srvOI.getListElement(arg[1].get(), 0));
    double upper2 = elemOI.get(srvOI.getListElement(arg[1].get(), 1));
    updateRet(lower1, lower2, upper1, upper2);
    return ret;
  }

  protected abstract void updateRet(double lower1, double lower2, double upper1, double upper2);

}
