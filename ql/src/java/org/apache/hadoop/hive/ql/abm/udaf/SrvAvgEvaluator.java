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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.ValueListParser;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SrvAvgEvaluator extends SrvEvaluatorWithInstruction {

  private final List<String> columnName = new ArrayList<String>(Arrays.asList("Group", "BaseSum",
      "BaseSsum", "BaseCnt"));

  private final List<ObjectInspector> objectInspectorType = new ArrayList<ObjectInspector>(
      Arrays.asList(
          (ObjectInspector) partialGroupOI,
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
          PrimitiveObjectInspectorFactory.javaIntObjectInspector));

  private final StructObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);

  private DoubleObjectInspector baseSumOI, baseSsumOI;
  private IntObjectInspector baseCntOI;
  private StructField sumField, ssumField, cntField;
  private SrvAvgComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
      sumField = fields.get(1);
      ssumField = fields.get(2);
      cntField = fields.get(3);
      baseSumOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
      baseSsumOI = (DoubleObjectInspector) ssumField.getFieldObjectInspector();
      baseCntOI = (IntObjectInspector) cntField.getFieldObjectInspector();
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return partialOI;
    } else {
      compute = new SrvAvgComputation();
      return doubleListOI;
    }
  }

  protected static class SrvAvgAggregationBuffer extends SrvAggregationBuffer {

    public double baseSum = 0;
    public double baseSsum = 0;
    public int baseCnt = 0;

    public SrvAvgAggregationBuffer(ValueListParser inputParser) {
      super(inputParser);
    }

    @Override
    public void processBase(double value) {
      baseSum += value;
      baseSsum += (value * value);
      baseCnt += 1;
    }

    public void processPartialBase(double sum, double ssum, int cnt) {
      baseSum += sum;
      baseSsum += ssum;
      baseCnt += cnt;
    }

    @Override
    public Object getPartialResult() {
      super.addGroupToRet();
      ret.add(baseSum);
      ret.add(baseSsum);
      ret.add(baseCnt);
      return ret;
    }

    @Override
    public void reset() {
      super.reset();
      baseSum = baseSsum = 0;
      baseCnt = 0;
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new SrvAvgAggregationBuffer(this.valueListParser);
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((SrvAvgAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  protected void parseBaseInfo(SrvAggregationBuffer agg, Object partialRes) {
    Object sumObj = mergeInputOI.getStructFieldData(partialRes, sumField);
    Object ssumObj = mergeInputOI.getStructFieldData(partialRes, ssumField);
    Object cntObj = mergeInputOI.getStructFieldData(partialRes, cntField);
    double sum = baseSumOI.get(sumObj);
    double ssum = baseSsumOI.get(ssumObj);
    int cnt = baseCntOI.get(cntObj);
    ((SrvAvgAggregationBuffer) agg).processPartialBase(sum, ssum, cnt);
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    SrvAvgAggregationBuffer myagg = (SrvAvgAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
    compute.setBase(myagg.baseSum, myagg.baseSsum, myagg.baseCnt);
    for (Map.Entry<Integer, DoubleArrayList> entry : myagg.groups.entrySet()) {

      compute.setCurrentList(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i++;
    }
    return compute.getFinalResult();

  }

}
