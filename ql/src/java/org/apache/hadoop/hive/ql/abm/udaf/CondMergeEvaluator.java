package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class CondMergeEvaluator extends GenericUDAFEvaluator {

  // protected ListObjectInspector condMatrixOI = (ListObjectInspector)
  // ConditionList.objectInspectorType;
  protected ListObjectInspector condArrayOI = (ListObjectInspector) ConditionList.arrayObjectInspectorType;
  protected StructObjectInspector condOI = ObjectInspectorFactory.getStandardStructObjectInspector(
      Condition.columnName, Condition.objectInspectorType);
  protected ListObjectInspector intListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  // protected StandardMapObjectInspector partialMapOI =
  // ObjectInspectorFactory.getStandardMapObjectInspector(intListOI,
  // ConditionGroup.groupObjectInspector);
  // protected StandardMapObjectInspector partialMapOI;

  //
  protected List<String> partialColumnName = Arrays.asList("Keys", "Values");
  protected ObjectInspector[] partialObjectInspector = {
      ObjectInspectorFactory.getStandardListObjectInspector(intListOI),
      ObjectInspectorFactory.getStandardListObjectInspector(ConditionGroup.groupObjectInspector)};

  private final KeyWrapper key = new KeyWrapper();

  protected Instruction ins = new Instruction();

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || (!(parameters[0] instanceof StandardStructObjectInspector))) {
      // return ObjectInspectorFactory.getStandardMapObjectInspector(intListOI,
      // ConditionGroup.groupObjectInspector);
      return ObjectInspectorFactory.getStandardStructObjectInspector(partialColumnName,
          Arrays.asList(partialObjectInspector));
    }
    else {
      // partialMapOI = (StandardMapObjectInspector)parameters[0];
      return ConditionList.objectInspectorType;
    }
  }

  static class MyAggregationBuffer implements AggregationBuffer {
    Map<IntArrayList, ConditionGroup> groups;
    int d;
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    myagg.groups.clear();
    myagg.d = 0;
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    MyAggregationBuffer myagg = new MyAggregationBuffer();
    myagg.groups = new HashMap<IntArrayList, ConditionGroup>();
    myagg.d = 0;
    return myagg;
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (parameters[0] != null) {
      /*
       * get the input Condition and ID
       */
      List<Condition> inputCondition = new ArrayList<Condition>();
      key.newKey();

      int colSize = ((ListObjectInspector) ConditionList.objectInspectorType)
          .getListLength(parameters[0]);

      for (int i = 0; i < colSize; i++) {
        Object arrayObj = ((ListObjectInspector) ConditionList.objectInspectorType).getListElement(
            parameters[0], i);
        Condition newCond = new Condition(
            ((ListObjectInspector) ConditionList.arrayObjectInspectorType).getListElement(arrayObj,
                0));
        inputCondition.add(newCond);
        key.add(newCond.getId());
      }

      // put the tuples in input Condition List to different groups
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      if (myagg.groups.containsKey(key)) {
        myagg.groups.get(key).add(inputCondition);
        // TODO set instruction here
      } else {
        ConditionGroup newgroup = new ConditionGroup(myagg.groups.size(), inputCondition.size());
        newgroup.add(inputCondition);
        myagg.groups.put(key.copyKey(), newgroup);
        // TODO set instruction here
      }
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

    /*
     * Hive Map does support Composite Key
     *
     * Map<Object, Object> ret = new HashMap<Object, Object>(myagg.groups.size());
     * for(Map.Entry<List<Integer>, ConditionGroup> entry: myagg.groups.entrySet())
     * {
     * ret.put(entry.getKey().toArray(), entry.getValue().toArray());
     * }
     */
    Object[] ret = new Object[2];
    Object[] keys = new Object[myagg.groups.size()];
    Object[] values = new Object[myagg.groups.size()];

    int i = 0;
    for (Map.Entry<IntArrayList, ConditionGroup> entry : myagg.groups.entrySet()) {
      System.out.println("During Terminate Partial: " + i);
      print(entry.getKey());
      entry.getValue().print();

      keys[i] = entry.getKey().toArray();
      values[i] = entry.getValue().toArray();
      i++;
    }
    ret[0] = keys;
    ret[1] = values;

    System.out.println("Terminate Partial: " + i);

    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
    if (partialRes != null) {
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      if (partialRes instanceof LazyBinaryStruct) {
        parseBinaryStruct(myagg, partialRes);
      } else {
        throw new UDFArgumentException("CondMerge: Unknown Data Type");
      }
    }
  }

  protected void parseBinaryStruct(MyAggregationBuffer myagg, Object partialRes) {
    LazyBinaryStruct binaryStruct = (LazyBinaryStruct) partialRes;

    LazyBinaryArray binaryKeys = (LazyBinaryArray) binaryStruct.getField(0);
    LazyBinaryArray binaryValues = (LazyBinaryArray) binaryStruct.getField(1);

    if (binaryKeys == null) {
      System.out.println(binaryKeys);
      return;
    }

    for (int i = 0; i < binaryKeys.getListLength(); i++) {
      LazyBinaryArray keyList = (LazyBinaryArray) binaryKeys.getListElementObject(i);
      LazyBinaryStruct condStruct = (LazyBinaryStruct) binaryValues.getListElementObject(i);

      key.newKey();
      for (int j = 0; j < keyList.getListLength(); j++) {
        key.add(((IntWritable) keyList.getListElementObject(j)).get());
      }

      ConditionGroup condGroup = ConditionGroup.getConditionGroup(condStruct);

      if (myagg.d == 0) {
        myagg.d = condGroup.getDimension();
      }

      if (myagg.groups.containsKey(key)) {
        myagg.groups.get(key).addAll(condGroup.getConditions());
        // TODO
      } else {
        // group not exists, create a new one
        condGroup.setGroupID(myagg.groups.size());
        myagg.groups.put(key.copyKey(), condGroup);
        // TODO
      }

    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    // for every group in groups
    // create a dimension sorting for every dimension
    // create partition, exhaustively find possible partition
    // output partition and conditions as ConditionList
    ConditionList newCondList = new ConditionList(myagg.d);

    for (Map.Entry<IntArrayList, ConditionGroup> entry : myagg.groups.entrySet()) {
      ConditionGroup condGroup = entry.getValue();
      condGroup.sort();

      System.out.println("During Terminate: " + entry.getValue().getGroupID());
      print(entry.getKey());
      entry.getValue().print();

      condGroup.genConditions(entry.getKey(), newCondList);
    }

    newCondList.print();

    return newCondList.toArray();
  }

}
