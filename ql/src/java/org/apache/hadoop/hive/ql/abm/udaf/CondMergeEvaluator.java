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

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapperParser;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeMatrixParser;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class CondMergeEvaluator extends GenericUDAFEvaluatorWithInstruction {

  protected StructObjectInspector inputOI;
  protected ListObjectInspector keyGroupOI, rangeGroupOI;
  protected StructField keyField;
  protected StructField rangeField;

  protected static List<String> columnNames = new ArrayList<String>(Arrays.asList("Keys", "Ranges"));
  protected final static ObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(
          columnNames, new ArrayList<ObjectInspector>(Arrays.asList(
              (ObjectInspector) ObjectInspectorFactory
                  .getStandardListObjectInspector(CondList.intListOI),
              ObjectInspectorFactory.getStandardListObjectInspector(CondList.doubleMatrixOI)))
      );
  protected final IntArrayList key = new IntArrayList();

  protected KeyWrapperParser keyParser = null;
  protected RangeMatrixParser rangeParser = null;

  protected ConditionComputation compute = null;

  protected List<Boolean> flags = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (parameters.length == 0) {
      return null;
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.STRUCT) {
      throw new UDFArgumentLengthException("CondMerge: Incorrect Input Type");
    }
    inputOI = (StructObjectInspector) parameters[0];

    List<? extends StructField> fields = inputOI.getAllStructFieldRefs();
    keyField = fields.get(0);
    rangeField = fields.get(1);

    if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
      keyParser = new KeyWrapperParser(keyField.getFieldObjectInspector());
      rangeParser = new RangeMatrixParser(rangeField.getFieldObjectInspector());
    } else {
      keyGroupOI = (ListObjectInspector) keyField.getFieldObjectInspector();
      rangeGroupOI = (ListObjectInspector) rangeField.getFieldObjectInspector();
      keyParser = new KeyWrapperParser(keyGroupOI.getListElementObjectInspector());
      rangeParser = new RangeMatrixParser(rangeGroupOI.getListElementObjectInspector());
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      // partialTerminate() will be called
      return partialOI;
    } else {
      return CondList.condListOI;
    }

  }

  // //
  // protected void fakeFlags() {
  // setFlags(Arrays.asList(true));
  // }

  public void setFlags(List<Boolean> flags) {
    this.flags = flags;
    compute = new ConditionComputation(flags);
  }

  protected static class MyAggregationBuffer implements AggregationBuffer {
    Map<IntArrayList, List<RangeList>> groups = new LinkedHashMap<IntArrayList, List<RangeList>>();
    Map<IntArrayList, Integer> keyIndexes = new LinkedHashMap<IntArrayList, Integer>();

    Object[] partialRet = new Object[2];
    List<Object> keyRet = new ArrayList<Object>();
    List<Object> rangeRet = new ArrayList<Object>();

    private RangeMatrixParser parser = null;

    public MyAggregationBuffer(RangeMatrixParser rangeParser) {
      parser = rangeParser;
    }

    public void reset() {
      groups.clear();
      keyIndexes.clear();
    }

    public int addRanges(IntArrayList key, Object o) {
      List<RangeList> ranges = groups.get(key);
      int index;

      if (ranges != null) {
        parser.parseInto(o, ranges);
        index = keyIndexes.get(key);
      } else {
        ranges = parser.parse(o);
        IntArrayList newKey = key.clone();
        index = keyIndexes.size();
        keyIndexes.put(newKey, index);
        groups.put(newKey, ranges);
      }

      return index;
    }

    public Object getPartialObj() {
      keyRet.clear();
      rangeRet.clear();
      for (Map.Entry<IntArrayList, List<RangeList>> entry : groups.entrySet()) {
        keyRet.add(entry.getKey());
        rangeRet.add(entry.getValue());
      }

      partialRet[0] = keyRet;
      partialRet[1] = rangeRet;
      return partialRet;
    }

    public void status(String function) {

      System.out.println("--------------------------------");
      System.out.println(function);
      for (Map.Entry<IntArrayList, List<RangeList>> entry : groups.entrySet()) {
        IntArrayList keyArray = entry.getKey();
        List<RangeList> rangeMatrix = entry.getValue();
        System.out.println("Key");
        for (long key : keyArray) {
          System.out.print(key + ",");
        }
        System.out.println();
        System.out.println("Range");
        for (RangeList list : rangeMatrix) {
          for (double range : list) {
            System.out.print(range + ",");
          }
          System.out.println();
        }
        System.out.println();
      }
      System.out.println("--------------------------------");
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MyAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new MyAggregationBuffer(rangeParser);
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (parameters[0] != null) {
      ins.resetGroupInstruction();

      Object rangeObj = inputOI.getStructFieldData(parameters[0], rangeField);
      boolean isBase = rangeParser.isBase(rangeObj);
      if (isBase) {
        ins.addGroupInstruction(-1);
        // System.out.println("Iterate Ins " + -1);
        return;
      }

      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      key.clear();
      keyParser.parseInto(inputOI.getStructFieldData(parameters[0], keyField), key);

      // Put the tuples in input Condition List to different groups
      int inst = myagg.addRanges(key, rangeObj);
      // Set the instruction here
      ins.addGroupInstruction(inst);

    }
  }

  protected void print(List<Integer> list) {
    System.out.print("Print List:");
    for (Integer number : list) {
      System.out.print(number + "\t");
    }
    System.out.println();
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    // myagg.status("terminatePartial");
    return myagg.getPartialObj();
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
    if (partialRes == null) {
      return;
    }
    ins.resetGroupInstruction();

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    Object keyGroupObj = inputOI.getStructFieldData(partialRes, this.keyField);
    Object rangeGroupObj = inputOI.getStructFieldData(partialRes, this.rangeField);

    for (int i = 0; i < this.keyGroupOI.getListLength(keyGroupObj); i++) {
      Object keyObj = this.keyGroupOI.getListElement(keyGroupObj, i);
      Object rangeObj = this.rangeGroupOI.getListElement(rangeGroupObj, i);

      key.clear();
      keyParser.parseInto(keyObj, key);

      int inst = myagg.addRanges(key, rangeObj);
      ins.addGroupInstruction(inst);
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    ins.resetMergeInstruction();

    for (Map.Entry<IntArrayList, List<RangeList>> entry : myagg.groups.entrySet()) {
      IntArrayList keyArray = entry.getKey();
      List<RangeList> rangeMatrix = entry.getValue();
      compute.setFields(keyArray, rangeMatrix);

      Merge merge = new Merge(flags, rangeMatrix);
      ins.addMergeInstruction(merge);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();
  }

}
