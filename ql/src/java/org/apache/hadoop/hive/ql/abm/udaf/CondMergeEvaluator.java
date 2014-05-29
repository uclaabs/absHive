package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;


public class CondMergeEvaluator extends GenericUDAFEvaluator {

  private final ListObjectInspector keyArrayOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  private final ListObjectInspector keyObjOI = ObjectInspectorFactory.getStandardListObjectInspector(keyArrayOI);
  private final StructObjectInspector rangeOI = (StructObjectInspector) ConditionRange.conditionRangeInspector;
  private final ListObjectInspector rangeArrayOI = ObjectInspectorFactory.getStandardListObjectInspector(rangeOI);
  private final ListObjectInspector rangeMatrixOI = ObjectInspectorFactory.getStandardListObjectInspector(rangeArrayOI);
  private final ListObjectInspector rangeObjOI = ObjectInspectorFactory.getStandardListObjectInspector(rangeMatrixOI);
  
  private final List<String> columnName = Arrays.asList("Keys", "Values");
  private final List<ObjectInspector> partialObjectInspectorType = Arrays.asList((ObjectInspector)keyObjOI, (ObjectInspector)rangeObjOI);
  private final StructObjectInspector partialOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnName, partialObjectInspectorType);
  
  private final StructObjectInspector condOI = CondGroup.condGroupInspector;
  private final IntObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

  private final KeyWrapper key = new KeyWrapper();
  private final List<ConditionRange> inputRange = new ArrayList<ConditionRange>();
  protected Instruction ins = new Instruction();

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);
    
    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return this.partialOI;
    } else {
      return this.condOI;
    }
  }

  private static class MyAggregationBuffer implements AggregationBuffer {
    Map<IntArrayList, List<List<ConditionRange>>> groups =
        new LinkedHashMap<IntArrayList, List<List<ConditionRange>>>();
    Map<IntArrayList, Integer> keyIndexes = new HashMap<IntArrayList, Integer>();

    public void reset() {
      groups.clear();
      keyIndexes.clear();
    }

    public int addRange(KeyWrapper key, List<ConditionRange> inputRange) {
      List<List<ConditionRange>> rangeGroup = groups.get(key);
      int index;

      if (rangeGroup != null) {
        for (int i = 0; i < inputRange.size(); i++) {
          rangeGroup.get(i).add(inputRange.get(i));
        }
        index = keyIndexes.get(key);
      } else {
        rangeGroup = new ArrayList<List<ConditionRange>>();
        for (int i = 0; i < inputRange.size(); i++) {
          List<ConditionRange> arrayRange = new ArrayList<ConditionRange>();
          arrayRange.add(inputRange.get(i));
          rangeGroup.add(arrayRange);
        }
        IntArrayList newKey = key.copyKey();
        index = groups.size();
        keyIndexes.put(newKey, index);
        groups.put(newKey, rangeGroup);
      }

      return index;
    }

    public int addRanges(KeyWrapper key, LazyBinaryArray inputRanges) {
      List<List<ConditionRange>> rangeGroup = groups.get(key);
      int index;

      if (rangeGroup != null) {
        for (int i = 0; i < inputRanges.getListLength(); i++) {
          LazyBinaryArray lazyArray = (LazyBinaryArray) inputRanges.getListElementObject(i);
          for (int j = 0; j < lazyArray.getListLength(); j++) {
            LazyBinaryStruct condObj = (LazyBinaryStruct) lazyArray.getListElementObject(j);
            rangeGroup.get(i).add(new ConditionRange(condObj));
          }
        }
        index = keyIndexes.get(key);
      } else {
        rangeGroup = new ArrayList<List<ConditionRange>>();
        for (int i = 0; i < inputRanges.getListLength(); i++) {
          LazyBinaryArray lazyArray = (LazyBinaryArray) inputRanges.getListElementObject(i);
          List<ConditionRange> newConds = new ArrayList<ConditionRange>();
          rangeGroup.add(newConds);
          for (int j = 0; j < lazyArray.getListLength(); j++) {
            LazyBinaryStruct condObj = (LazyBinaryStruct) lazyArray.getListElementObject(j);
            newConds.add(new ConditionRange(condObj));
          }
        }
        IntArrayList newKey = key.copyKey();
        index = groups.size();
        keyIndexes.put(newKey, index);
        groups.put(newKey, rangeGroup);
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
      // Get the input Condition and ID
      key.newKey();
      inputRange.clear();

      Object keyObj = this.condOI.getStructFieldData(parameters[0], this.condOI.getStructFieldRef("Keys"));
      Object rangeMatrixObj = this.condOI.getStructFieldData(parameters[0], this.condOI.getStructFieldRef("Values"));

//      // Assume there is only one key array in input
//      Object keyObj = this.keyObjOI.getListElement(keysObj, 0);
//      Object rangeMatrixObj = this.rangeObjOI.getListElement(rangesObj, 0);

      for (int i = 0; i < this.keyArrayOI.getListLength(keyObj); i++) {
        key.add(intOI.get(this.keyArrayOI.getListElement(keyObj, i)));
      }

      boolean baseFlag = true;
      for (int i = 0; i < this.rangeMatrixOI.getListLength(rangeMatrixObj); i++) {
        Object rangeArrayObj = this.rangeMatrixOI.getListElement(rangeMatrixObj, i);
        ConditionRange newConditionRange = new ConditionRange(this.rangeArrayOI.getListElement(
            rangeArrayObj, 0));
        baseFlag = newConditionRange.isBase();
        inputRange.add(newConditionRange);
      }
      if (baseFlag) {
        ins.addGroupInstruction(-1);
        return;
      }

      // Put the tuples in input Condition List to different groups
      // Set the instruction here
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      ins.addGroupInstruction(myagg.addRange(key, inputRange));
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
    for (Map.Entry<IntArrayList, List<List<ConditionRange>>> entry : myagg.groups.entrySet()) {
      keys[i] = entry.getKey().toArray();
      List<List<ConditionRange>> rangeGroup = entry.getValue();

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

    return new Object[] { keys, values };
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
    
    if (partialRes == null) {
      return;
    }

    if (!(partialRes instanceof LazyBinaryStruct)) {
      throw new UDFArgumentException("CondMerge: Unknown Data Type");
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

      key.newKey();
      for (int j = 0; j < keyList.getListLength(); j++) {
        key.add(((IntWritable) keyList.getListElementObject(j)).get());
      }

      ins.addGroupInstruction(myagg.addRanges(key, condRangeMatrix));
    }
  }


  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    ConditionComputation comp = new ConditionComputation();

    boolean set = false;
    for (Map.Entry<IntArrayList, List<List<ConditionRange>>> entry : myagg.groups.entrySet()) {
      IntArrayList keyArray = entry.getKey();
      List<List<ConditionRange>> rangeMatrix = entry.getValue();
      if (!set) {
        comp.setCondGroup(rangeMatrix.size());
        set = true;
      }
      comp.setFields(keyArray, rangeMatrix);

      Merge merge = new Merge();
      for (List<ConditionRange> rangeArray : rangeMatrix) {
        merge.addDimension(rangeArray);
      }

      ins.addMergeInstruction(merge);
      comp.setFlags(merge.getFlags());
      merge.enumerate(comp);
    }

    return comp.getFinalResult();
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
