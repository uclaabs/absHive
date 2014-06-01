package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;

public class CondMergeEvaluator extends GenericUDAFEvaluatorWithInstruction {

  private StructObjectInspector inputOI;
  private ListObjectInspector keyOI, rangeOI, rangeListOI, keyGroupOI, rangeGroupOI;
  private LongObjectInspector keyValueOI;
  private DoubleObjectInspector rangeValueOI;
  private StructField keyField;
  private StructField rangeField;

  private static ListObjectInspector longMatrixOI = ObjectInspectorFactory
      .getStandardListObjectInspector(CondList.longListOI);
  private static ListObjectInspector doubleMatrixOI = ObjectInspectorFactory
      .getStandardListObjectInspector(CondList.doubleMatrixOI);
  private static List<String> columnName = Arrays.asList("Keys", "Ranges");
  private static List<ObjectInspector> ObjectInspectorType = Arrays.asList(
      (ObjectInspector) longMatrixOI, (ObjectInspector) doubleMatrixOI);
  private static StructObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, ObjectInspectorType);

  private final KeyWrapper key = new KeyWrapper();

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (parameters[0].getCategory() != ObjectInspector.Category.STRUCT) {
      throw new UDFArgumentLengthException("CondMerge: Incorrect Input Type");
    }
    inputOI = (StructObjectInspector) parameters[0];
    keyField = inputOI.getStructFieldRef("Keys");
    rangeField = inputOI.getStructFieldRef("Ranges");

    if (m == Mode.PARTIAL1 || m == Mode.FINAL) {
      keyOI = (ListObjectInspector) keyField.getFieldObjectInspector();
      rangeOI = (ListObjectInspector) rangeField.getFieldObjectInspector();
    } else {
      keyGroupOI = (ListObjectInspector) keyField.getFieldObjectInspector();
      rangeGroupOI = (ListObjectInspector) rangeField.getFieldObjectInspector();
      keyOI = (ListObjectInspector) keyGroupOI.getListElementObjectInspector();
      rangeOI = (ListObjectInspector) rangeGroupOI.getListElementObjectInspector();
    }

    rangeListOI = (ListObjectInspector) rangeOI.getListElementObjectInspector();
    keyValueOI = (LongObjectInspector) keyOI.getListElementObjectInspector();
    rangeValueOI = (DoubleObjectInspector) rangeListOI.getListElementObjectInspector();


    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      // partialTerminate() will be called
      return partialOI;
    } else {
      return CondList.condListOI;
    }

  }

  private static class MyAggregationBuffer implements AggregationBuffer {
    Map<KeyWrapper, List<RangeList>> groups = new LinkedHashMap<KeyWrapper, List<RangeList>>();
    Map<KeyWrapper, Integer> keyIndexes = new LinkedHashMap<KeyWrapper, Integer>();
    ConditionComputation compute = new ConditionComputation();
    Object[] partialRet = new Object[2];
    List<Object> keyRet = new ArrayList<Object>();
    List<Object> rangeRet = new ArrayList<Object>();

    ListObjectInspector rangeListOI;
    DoubleObjectInspector rangeValueOI;

    public MyAggregationBuffer(ListObjectInspector rangeList, ObjectInspector rangeValOI) {
      this.rangeValueOI = (DoubleObjectInspector) rangeValOI;
      this.rangeListOI = rangeList;
    }

    public void reset() {
      groups.clear();
      keyIndexes.clear();
      compute.clear();
    }

    public int addRanges(KeyWrapper key, Object o, ListObjectInspector loi) {
      List<RangeList> ranges = groups.get(key);
      int index;

      int length = loi.getListLength(o);
      if (ranges != null) {
        for (int i = 0; i < length; ++i) {
          Object listObj = loi.getListElement(o, i);
          for (int j = 0; j < rangeListOI.getListLength(listObj); j++) {
            ranges.get(i).add(rangeValueOI.get(rangeListOI.getListElement(listObj, j)));
          }
        }
        index = keyIndexes.get(key);
      } else {
        ranges = new ArrayList<RangeList>();
        for (int i = 0; i < length; ++i) {
          RangeList rl = new RangeList();
          Object listObj = loi.getListElement(o, i);
          for (int j = 0; j < rangeListOI.getListLength(listObj); j++) {
            rl.add(rangeValueOI.get(rangeListOI.getListElement(listObj, j)));
          }
          ranges.add(rl);
        }
        KeyWrapper newKey = key.copyKey();
        index = keyIndexes.size();
        keyIndexes.put(newKey, index);
        groups.put(newKey, ranges);
      }

      return index;
    }

    public Object getPartialObj() {
      keyRet.clear();
      rangeRet.clear();
      //
      for (Map.Entry<KeyWrapper, List<RangeList>> entry : groups.entrySet()) {
        keyRet.add(entry.getKey());
        rangeRet.add(entry.getValue());
      }
      partialRet[0] = keyRet;
      partialRet[1] = rangeRet;
      return partialRet;
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MyAggregationBuffer) agg).reset();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new MyAggregationBuffer(this.rangeListOI, this.rangeValueOI);
  }

  protected boolean isBase(Object rangeObj, int i) {
    double lower = this.rangeValueOI.get(this.rangeListOI.getListElement(rangeObj, 0));
    double upper = this.rangeValueOI.get(this.rangeListOI.getListElement(rangeObj, 1));
    if (lower == Double.NEGATIVE_INFINITY && upper == Double.POSITIVE_INFINITY) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (parameters[0] != null) {
      Object keyObj = this.inputOI.getStructFieldData(parameters[0], this.keyField);
      Object rangeObj = this.inputOI.getStructFieldData(parameters[0], this.rangeField);

      boolean isBase = true;
      for (int i = 0; i < this.rangeOI.getListLength(rangeObj); i++) {
        Object rangeListObj = this.rangeOI.getListElement(rangeObj, i);
        if (isBase(rangeListObj, i)) {
          isBase = false;
          break;
        }
      }
      if (isBase) {
        ins.addGroupInstruction(-1);
        return;
      }

      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      key.parseKey(keyObj, keyOI, keyValueOI);

      // Put the tuples in input Condition List to different groups
      int inst = myagg.addRanges(key, rangeObj, this.rangeOI);
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
    return myagg.getPartialObj();

    // Object[] keys = new Object[myagg.groups.size()];
    // Object[] values = new Object[myagg.groups.size()];
    //
    // int i = 0;
    // for (Map.Entry<KeyWrapper, List<RangeList>> entry : myagg.groups.entrySet()) {
    // keys[i] = entry.getKey().toArray();
    // List<RangeList> rangeGroup = entry.getValue();
    //
    // // TODO:
    // Object[] rangeMatrix = new Object[rangeGroup.size()];
    // for (int j = 0; j < rangeGroup.size(); j++) {
    // Object[] rangeArray = new Object[rangeGroup.get(j).size()];
    // List<ConditionRange> rangeList = rangeGroup.get(j);
    // for (int k = 0; k < rangeList.size(); k++) {
    // rangeArray[k] = rangeList.get(k).toArray();
    // }
    // rangeMatrix[j] = rangeArray;
    // }
    // values[i] = rangeMatrix;
    // ++i;
    // }
    //
    // return new Object[] {keys, values};

  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
    if (partialRes == null) {
      return;
    }

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    Object keyGroupObj = this.inputOI.getStructFieldData(partialRes, this.keyField);
    Object rangeGroupObj = this.inputOI.getStructFieldData(partialRes, this.rangeField);

    for (int i = 0; i < this.keyGroupOI.getListLength(keyGroupObj); i++) {
      Object keyObj = this.keyGroupOI.getListElement(keyGroupObj, i);
      Object rangeObj = this.rangeGroupOI.getListElement(rangeGroupObj, i);

      key.parseKey(keyObj, keyOI, keyValueOI);

      int inst = myagg.addRanges(key, rangeObj, this.rangeOI);
      ins.addGroupInstruction(inst);
    }

  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    ConditionComputation compute = myagg.compute;

    boolean set = false;
    for (Map.Entry<KeyWrapper, List<RangeList>> entry : myagg.groups.entrySet()) {
      KeyWrapper keyArray = entry.getKey();
      List<RangeList> rangeMatrix = entry.getValue();
      if (!set) {
        compute.setCondGroup(rangeMatrix.size());
        set = true;
      }
      compute.setFields(keyArray, rangeMatrix);

      Merge merge = new Merge();
      for (RangeList rangeArray : rangeMatrix) {
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
