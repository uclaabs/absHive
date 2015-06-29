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

package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CaseCountEvaluator extends SrvCountEvaluator {

  private CaseCountComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if(m == Mode.PARTIAL2 || m == Mode.FINAL) {
      partialResOI = (LongObjectInspector) parameters[0];
    }
    
    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else {
      compute = new CaseCountComputation();
      return doubleListOI;
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    compute.setCount(myagg.baseCnt);
    for(int i = 0; i < instructions.size(); i ++) {
      compute.addNewGroup();
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();

  }

}
