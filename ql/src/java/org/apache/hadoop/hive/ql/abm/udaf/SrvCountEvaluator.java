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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

public class SrvCountEvaluator extends GenericUDAFEvaluatorWithInstruction {

  protected final ListObjectInspector doubleListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);

  protected final LongWritable ret = new LongWritable(0);
  protected LongObjectInspector partialResOI = null;

  private SrvCountComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
      partialResOI = (LongObjectInspector) parameters[0];
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else {
      compute = new SrvCountComputation();
      return doubleListOI;
    }
  }

  protected static class MyAggregationBuffer implements AggregationBuffer {

    long baseCnt = 0;

    public void addBase(long cnt) {
      baseCnt += cnt;
    }

    public void reset() {
      baseCnt = 0;
    }

  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new MyAggregationBuffer();
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MyAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (ins.getGroupInstruction().size() > 0) {
      int instruction = ins.getGroupInstruction().getInt(0);
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

      if (instruction == -1) {
        myagg.addBase(1);
      }
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    ret.set(((MyAggregationBuffer) agg).baseCnt);
    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    myagg.addBase(partialResOI.get(partial));
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    compute.setTotalTupleNumber(N);
    compute.setCount(myagg.baseCnt);
    List<Merge> instructions = ins.getMergeInstruction();

    for (int i = 0; i < instructions.size(); i++) {
      compute.addNewGroup();
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();
  }

}
