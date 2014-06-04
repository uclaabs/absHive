package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
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

  protected static List<String> columnNames = Arrays.asList("Keys", "Ranges");
  protected final KeyWrapper key = new KeyWrapper();

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
      return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, Arrays.asList(
          (ObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(CondList.intListOI),
            ObjectInspectorFactory.getStandardListObjectInspector(CondList.doubleMatrixOI))
          );
    } else {
      return CondList.condListOI;
    }

  }

//  //
//  protected void fakeFlags() {
//    setFlags(Arrays.asList(true));
//  }

  public void setFlags(List<Boolean> flags) {
    this.flags = flags;
    compute = new ConditionComputation(this.flags.size());
  }

  protected static class MyAggregationBuffer implements AggregationBuffer {
    Map<KeyWrapper, List<RangeList>> groups = new LinkedHashMap<KeyWrapper, List<RangeList>>();
    Map<KeyWrapper, Integer> keyIndexes = new LinkedHashMap<KeyWrapper, Integer>();

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

    public int addRanges(KeyWrapper key, Object o) {
      List<RangeList> ranges = groups.get(key);
      int index;

      if (ranges != null) {
        parser.parseInto(o, ranges);
        index = keyIndexes.get(key);
      } else {
        ranges = parser.parse(o);
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
      for (Map.Entry<KeyWrapper, List<RangeList>> entry : groups.entrySet()) {
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
      for (Map.Entry<KeyWrapper, List<RangeList>> entry : groups.entrySet()) {
        KeyWrapper keyArray = entry.getKey();
        List<RangeList> rangeMatrix = entry.getValue();
        System.out.println("Key");
        for (long key : keyArray) {
          System.out.print(key + "\t");
        }
        System.out.println();
        System.out.println("Range");
        for (RangeList list : rangeMatrix) {
          for (double range : list) {
            System.out.print(range + "\t");
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
//        System.out.println("Iterate " + -1);
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
    ins.resetMergeInstruction();

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

    for (Map.Entry<KeyWrapper, List<RangeList>> entry : myagg.groups.entrySet()) {
      KeyWrapper keyArray = entry.getKey();
      List<RangeList> rangeMatrix = entry.getValue();
      compute.setFields(keyArray, rangeMatrix);

      Merge merge = new Merge(flags, rangeMatrix);
      ins.addMergeInstruction(merge);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();
  }

}
