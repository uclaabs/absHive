package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CondMergeEvaluator extends GenericUDAFEvaluatorWithInstruction {

  private final ListObjectInspector keyArrayOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  private final ListObjectInspector keyObjOI = ObjectInspectorFactory
      .getStandardListObjectInspector(keyArrayOI);
  private final StructObjectInspector rangeOI = (StructObjectInspector) ConditionRange.conditionRangeInspector;
  private final ListObjectInspector rangeArrayOI = ObjectInspectorFactory
      .getStandardListObjectInspector(rangeOI);
  private final ListObjectInspector rangeMatrixOI = ObjectInspectorFactory
      .getStandardListObjectInspector(rangeArrayOI);
  private final ListObjectInspector rangeObjOI = ObjectInspectorFactory
      .getStandardListObjectInspector(rangeMatrixOI);

  private final List<String> columnName = Arrays.asList("Keys", "Values");
  private final List<ObjectInspector> partialObjectInspectorType = Arrays.asList(
      (ObjectInspector) keyObjOI, (ObjectInspector) rangeObjOI);
  private final StructObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, partialObjectInspectorType);

  private final StructObjectInspector condOI = CondGroup.condGroupInspector;

  private final KeyWrapper key = new KeyWrapper();

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    // TODO


    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return this.partialOI;
    } else {
      return this.condOI;
    }
  }

  private static class MyAggregationBuffer implements AggregationBuffer {
    Map<KeyWrapper, List<RangeList>> groups = new HashMap<KeyWrapper, List<RangeList>>();
    Map<IntArrayList, Integer> keyIndexes = new HashMap<IntArrayList, Integer>();
    ConditionComputation compute = new ConditionComputation();



    public void reset() {
      groups.clear();
      keyIndexes.clear();
      compute.clear();
    }

    public int addRanges(KeyWrapper key, Object o, ListObjectInspector loi) {
      List<RangeList> ranges = groups.get(key);
      int index;

      ListObjectInspector eoi = (ListObjectInspector) loi.getListElementObjectInspector();
      int length = loi.getListLength(o);

      if (ranges != null) {
        for (int i = 0; i < length; ++i) {
          ranges.get(i).addAll(loi.getListElement(o, i), eoi);
        }
        index = keyIndexes.get(key);
      } else {
        ranges = new ArrayList<RangeList>();
        for (int i = 0; i < length; ++i) {
          RangeList rl = new RangeList();
          rl.addAll(loi.getListElement(o, i), eoi);
          ranges.add(rl);
        }
        KeyWrapper newKey = key.copyKey();
        index = keyIndexes.size();
        keyIndexes.put(newKey, index);
        groups.put(newKey, ranges);
      }

      return index;
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MyAggregationBuffer) agg).reset();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new MyAggregationBuffer();
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (parameters[0] != null) {
      // TODO: reuse condOI.getStructFieldRef
      //condOI.getAllStructFieldRefs();
      Object keyObj = this.condOI.getStructFieldData(parameters[0],
          this.condOI.getStructFieldRef("Keys"));
      Object rangeMatrixObj = this.condOI.getStructFieldData(parameters[0],
          this.condOI.getStructFieldRef("Values"));

      boolean isBase = true;
      for (int i = 0; i < this.rangeMatrixOI.getListLength(rangeMatrixObj); i++) {
        Object rangeArrayObj = this.rangeMatrixOI.getListElement(rangeMatrixObj, i);
        if (!ConditionRange.isBase(this.rangeArrayOI.getListElement(
            rangeArrayObj, 0))) {
          isBase = false;
          break;
        }
      }

      if (isBase) {
        ins.addGroupInstruction(-1);
        return;
      }

      // Otherwise...
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      // Get the input id
      key.parseKey(keyObj, keyArrayOI);
      // Put the tuples in input Condition List to different groups
      int inst = myagg.addRanges(key, rangeMatrixObj, rangeMatrixOI);
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

    Object[] keys = new Object[myagg.groups.size()];
    Object[] values = new Object[myagg.groups.size()];

    int i = 0;
    for (Map.Entry<KeyWrapper, List<RangeList>> entry : myagg.groups.entrySet()) {
      keys[i] = entry.getKey().toArray();
      List<RangeList> rangeGroup = entry.getValue();

      // TODO:
      Object[] rangeMatrix = new Object[rangeGroup.size()];
      for (int j = 0; j < rangeGroup.size(); j++) {
        Object[] rangeArray = new Object[rangeGroup.get(j).size()];
        List<ConditionRange> rangeList = rangeGroup.get(j);
        for (int k = 0; k < rangeList.size(); k++) {
          rangeArray[k] = rangeList.get(k).toArray();
        }
        rangeMatrix[j] = rangeArray;
      }
      values[i] = rangeMatrix;
      ++i;
    }

    return new Object[] {keys, values};
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
    if (partialRes == null) {
      return;
    }

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    LazyBinaryStruct binaryStruct = (LazyBinaryStruct) partialRes;
    LazyBinaryArray binaryKeys = (LazyBinaryArray) binaryStruct.getField(0);
    LazyBinaryArray binaryValues = (LazyBinaryArray) binaryStruct.getField(1);

    int numEntries = binaryKeys.getListLength(); // Number of map entry
    for (int i = 0; i < numEntries; i++) {
      // For every entry, the keyList and rangeMatrix
      LazyBinaryArray keyList = (LazyBinaryArray) binaryKeys.getListElementObject(i);
      LazyBinaryArray condRangeMatrix = (LazyBinaryArray) binaryValues.getListElementObject(i);

      key.parseKey(keyList, keyArrayOI);
      int inst = myagg.addRanges(key, condRangeMatrix, rangeMatrixOI);
      ins.addGroupInstruction(inst);
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    ConditionComputation compute = myagg.compute;

    boolean set = false;
    for (Map.Entry<IntArrayList, List<List<ConditionRange>>> entry : myagg.groups.entrySet()) {
      IntArrayList keyArray = entry.getKey();
      List<List<ConditionRange>> rangeMatrix = entry.getValue();
      if (!set) {
        compute.setCondGroup(rangeMatrix.size());
        set = true;
      }
      compute.setFields(keyArray, rangeMatrix);

      Merge merge = new Merge();
      for (List<ConditionRange> rangeArray : rangeMatrix) {
        merge.addDimension(rangeArray);
      }

      ins.addMergeInstruction(merge);
      compute.setFlags(merge.getFlags());
      merge.enumerate(compute);
    }

    return compute.getFinalResult();
  }

}

class DummyCondMergeEvaluator extends CondMergeEvaluator {

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    ins.addGroupInstruction(-1);
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    return null;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
  }

}
