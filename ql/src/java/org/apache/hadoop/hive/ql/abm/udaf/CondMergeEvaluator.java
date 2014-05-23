package org.apache.hadoop.hive.ql.abm.udaf;

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

public class CondMergeEvaluator extends GenericUDAFEvaluator{

  // protected ListObjectInspector condMatrixOI = (ListObjectInspector) ConditionList.objectInspectorType;
  protected ListObjectInspector condArrayOI = (ListObjectInspector) ConditionList.arrayObjectInspectorType;
  protected StructObjectInspector condOI =  ObjectInspectorFactory.getStandardStructObjectInspector(Condition.columnName, Condition.objectInspectorType);
  protected ListObjectInspector intListOI =  ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  // protected StandardMapObjectInspector partialMapOI = ObjectInspectorFactory.getStandardMapObjectInspector(intListOI, ConditionGroup.groupObjectInspector);
  // protected StandardMapObjectInspector partialMapOI;

  //
  protected List<String> partialColumnName = Arrays.asList("Keys", "Values");
  protected ObjectInspector[] partialObjectInspector = {ObjectInspectorFactory.getStandardListObjectInspector(intListOI), ObjectInspectorFactory.getStandardListObjectInspector(ConditionGroup.groupObjectInspector)};
  //

  protected Instruction ins = new Instruction();

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException
  {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || (!(parameters[0] instanceof StandardStructObjectInspector)))
    {
      // return ObjectInspectorFactory.getStandardMapObjectInspector(intListOI, ConditionGroup.groupObjectInspector);
      return ObjectInspectorFactory.getStandardStructObjectInspector(partialColumnName, Arrays.asList(partialObjectInspector));
    }
    else
    {
      // partialMapOI = (StandardMapObjectInspector)parameters[0];
      return  ConditionList.objectInspectorType;
    }
  }

  static class MyAggregationBuffer implements AggregationBuffer
  {
    Map<List<Integer>, ConditionGroup> groups;
    int d;
  }

//  protected void parseObjectMap(MyAggregationBuffer myagg, Object partialRes)
//  {
//    Object partialObj = this.partialMapOI.getMap(partialRes);
//    Map<Object, Object> mapObj = (Map<Object, Object>) partialObj;
//
//    for (Map.Entry<Object,Object> entry: mapObj.entrySet())
//    {
//      List<Integer> values = new ArrayList<Integer>();
//      Object listObj = entry.getKey();
//      Object groupObj = entry.getValue();
//
//      for(int i = 0; i < this.intListOI.getListLength(listObj); i ++)
//      {
//        // values.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector.get(objlist[i]));
//        Object intObj = this.intListOI.getListElement(listObj, i);
//        values.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector.get(intObj));
//      }
//
//      ConditionGroup condGroup = ConditionGroup.getConditionGroup(groupObj);
//
//      if(myagg.d == 0)
//        myagg.d = condGroup.getDimension();
//
//      if(myagg.groups.containsKey(values))
//      {
//        myagg.groups.get(values).addAll(condGroup.getConditions());
//        //TODO
//      }
//      else
//      {
//        // group not exists, create a new one
//        condGroup.setGroupID(myagg.groups.size());
//        myagg.groups.put(values, condGroup);
//        //TODO
//      }
//
//    }
//  }

  protected void parseBinaryStruct(MyAggregationBuffer myagg, Object partialRes)
  {
//    System.out.println("Parse Binary Struct;");
    LazyBinaryStruct binaryStruct = (LazyBinaryStruct)partialRes;

    LazyBinaryArray binaryKeys = (LazyBinaryArray)binaryStruct.getField(0);
    LazyBinaryArray binaryValues = (LazyBinaryArray)binaryStruct.getField(1);


    if(binaryKeys == null)
    {
      System.out.println(binaryKeys);
      return;
    }

    for(int i = 0; i < binaryKeys.getListLength(); i ++)
    {
      LazyBinaryArray keyList = (LazyBinaryArray)binaryKeys.getListElementObject(i);
      LazyBinaryStruct condStruct = (LazyBinaryStruct)binaryValues.getListElementObject(i);

      List<Integer> keys = new ArrayList<Integer>();
      for(int j = 0; j < keyList.getListLength(); j ++) {
        keys.add(((IntWritable)keyList.getListElementObject(j)).get());
      }

      ConditionGroup condGroup = ConditionGroup.getConditionGroup(condStruct);

      // condGroup.print();

      if(myagg.d == 0) {
        myagg.d = condGroup.getDimension();
      }

      if(myagg.groups.containsKey(keys))
      {
        myagg.groups.get(keys).addAll(condGroup.getConditions());
        //TODO
      }
      else
      {
        // group not exists, create a new one
        condGroup.setGroupID(myagg.groups.size());
        myagg.groups.put(keys, condGroup);
        //TODO
      }

    }

    System.out.println("Parse Over " + myagg.groups.size());

  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException
  {
    MyAggregationBuffer myagg = (MyAggregationBuffer)agg;
    myagg.groups.clear();
    myagg.d = 0;
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException
  {
    MyAggregationBuffer myagg = new MyAggregationBuffer();
    myagg.groups = new HashMap<List<Integer>, ConditionGroup>();
    myagg.d = 0;
    return myagg;
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException
  {
    if(parameters[0] != null)
    {
      /*
       * get the input Condition and ID
       */
      List<Condition> inputCondition = new ArrayList<Condition>();
      List<Integer> inputGroupID = new ArrayList<Integer>();

//      System.out.println("Iterate Start");

      int colSize = ((ListObjectInspector)ConditionList.objectInspectorType).getListLength(parameters[0]);
      for(int i = 0; i <  colSize; i ++)
      {
        Object arrayObj = ((ListObjectInspector)ConditionList.objectInspectorType).getListElement(parameters[0], i);
        Condition newCond = new Condition(((ListObjectInspector)ConditionList.arrayObjectInspectorType).getListElement(arrayObj, 0));
//        System.out.print(newCond.toString() + "\t");
        inputCondition.add(newCond);
        inputGroupID.add(newCond.getID());
      }

//      System.out.println();



      /*
       * put the tuples in input Condition List to different groups
       */
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      if(myagg.groups.containsKey(inputGroupID))
      {
        myagg.groups.get(inputGroupID).add(inputCondition);
      //TODO set instruction here
      }
      else
      {
        ConditionGroup newgroup = new ConditionGroup(myagg.groups.size(), inputCondition.size());
        newgroup.add(inputCondition);
        myagg.groups.put(inputGroupID, newgroup);
      //TODO set instruction here
      }

      System.out.println("Iterate End");
    }
  }

  protected void print(List<Integer> list)
  {
    System.out.print("Print List:");
    for(Integer number:list) {
      System.out.print(number + "\t");
    }
    System.out.println();
  }



  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException
  {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    /*
     * Hive Map does support Composite Key
     *
    Map<Object, Object> ret = new HashMap<Object, Object>(myagg.groups.size());
    for(Map.Entry<List<Integer>, ConditionGroup> entry: myagg.groups.entrySet())
    {
      ret.put(entry.getKey().toArray(), entry.getValue().toArray());
    }
    *
    */
    Object[] ret = new Object[2];
    Object[] keys = new Object[myagg.groups.size()];
    Object[] values = new Object[myagg.groups.size()];

    int i = 0;
    for(Map.Entry<List<Integer>, ConditionGroup> entry: myagg.groups.entrySet())
    {
      System.out.println("During Terminate Partial: " + i);
      print(entry.getKey());
      entry.getValue().print();

      keys[i] = entry.getKey().toArray();
      values[i] = entry.getValue().toArray();
      i ++;
    }
    ret[0] = keys;
    ret[1] = values;

    System.out.println("Terminate Partial: " + i);

    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException
  {

    if(partialRes != null)
    {
//      System.out.println("Merge Start");

      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      if(partialRes instanceof LazyBinaryStruct) {
        parseBinaryStruct(myagg, partialRes);
      } else {
        throw new UDFArgumentException("CondMerge: Unknown Data Type");
      }

//      System.out.println("Merge End");
    }

  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException
  {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    System.out.println("Terminate Start");

    // for every group in groups
    //    create a dimension sorting for every dimension
    //      create partition, exhaustively find possible partition
    //        output partition and conditions as ConditionList
    ConditionList newCondList = new ConditionList(myagg.d);

    for(Map.Entry<List<Integer>, ConditionGroup> entry: myagg.groups.entrySet())
    {
      ConditionGroup condGroup = entry.getValue();
      condGroup.sort();

      System.out.println("During Terminate: " + entry.getValue().getGroupID());
      print(entry.getKey());
      entry.getValue().print();

      condGroup.genConditions(entry.getKey(), newCondList);
    }

    System.out.println("Terminate End");
    newCondList.print();

    return newCondList.toArray();
  }

}

