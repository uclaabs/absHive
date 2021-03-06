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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@UDFType(deterministic = false)
@Description(name = "unix_timestamp",
    value = "_FUNC_([date[, pattern]]) - Returns the UNIX timestamp",
    extended = "Converts the current or specified time to number of seconds "
        + "since 1970-01-01.")
public class GenericUDFUnixTimeStamp extends GenericUDFToUnixTimeStamp {

  @Override
  protected void initializeInput(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 0) {
      super.initializeInput(arguments);
    }
  }

  @Override
  protected String getName() {
    return "unix_timestamp";
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments.length == 0) {
      retValue.set(System.currentTimeMillis() / 1000);
      return retValue;
    }
    return super.evaluate(arguments);
  }
}
