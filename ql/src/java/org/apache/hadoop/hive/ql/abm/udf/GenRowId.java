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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 *
 * GenRowId generates unique ID for each row.
 *
 */
@Description(name = "GenRowId", value = "_FUNC_() - Returns a unique ID (split ID + sequence ID)")
@UDFType(stateful = true)
public class GenRowId extends GenericUDF {

	private IntWritable id = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
	  if (arguments.length != 0) {
      throw new UDFArgumentException("This function takes no argument!");
    }
	  id = new IntWritable(0);
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  public void setSplitId(int split) {
    id.set(split << 16);
  }

	@Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
	  id.set(id.get() + 1);
    return id;
  }

  @Override
  public String getDisplayString(String[] arg0) {
	  return "GenRowId()";
  }

}
