package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/*
 * updated 05/28/2014, convert condGroup from Struct<List<List>, List<List<List>>> to Struct{List, List<List>}
 */
//TODO: reuse
public class CondGroup {

  List<Integer> key;
  List<List<ConditionRange>> range;

  public static List<String> columnNames = Arrays.asList("Keys", "Values");

  // public static ObjectInspector[] objectInspectors =
  // {ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector)),
  // ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(ConditionRange.conditionRangeInspector)))};

  public static ObjectInspector[] objectInspectors = {
      ObjectInspectorFactory
          .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector),
      ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
          .getStandardListObjectInspector(ConditionRange.conditionRangeInspector))};

  public static List<ObjectInspector> objectInspectorType = Arrays.asList(objectInspectors);

  public static StructObjectInspector condGroupInspector = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnNames, objectInspectorType);

  public CondGroup() {
    this.key = new ArrayList<Integer>();
    this.range = new ArrayList<List<ConditionRange>>();
  }

  public CondGroup(List<Integer> keyList, List<List<ConditionRange>> rangeMatrix) {
    this.key = keyList;
    this.range = rangeMatrix;
  }

  public void addKey(int keyValue) {
    this.key.add(keyValue);
  }

  public void addRangeList(List<ConditionRange> rangeList) {
    this.range.add(rangeList);
  }

  // public static void update(Object condGroupObj, int id, double start, double end)
  // {
  // Object[] ret = (Object[])condGroupObj;
  // ArrayList<Integer> keyArray = (ArrayList<Integer>)ret[0];
  // Object[] rangeMatrix = (Object[])ret[1];
  //
  // keyArray.set(0, id);
  // Object[] rangeArray = (Object[]) rangeMatrix[0];
  // Object[] range = (Object[])rangeArray[0];
  // range[0] = start;
  // range[1] = end;
  // }

  @SuppressWarnings("unchecked")
  public static void update(Object condGroupObj, int id, double start, double end) {
    ArrayList<Object> ret = (ArrayList<Object>) condGroupObj;
    ArrayList<Integer> keyArray = (ArrayList<Integer>) ret.get(0);
    ArrayList<Object> rangeMatrix = (ArrayList<Object>) ret.get(1);

    keyArray.set(0, id);
    ArrayList<Object> rangeArray = (ArrayList<Object>) rangeMatrix.get(0);
    ArrayList<Object> range = (ArrayList<Object>) rangeArray.get(0);
    range.set(0, start);
    range.set(1, end);
  }

  @SuppressWarnings("unchecked")
  public static void update(Object condGroupObj, int id1, int id2, double start, double end) {
    ArrayList<Object> ret = (ArrayList<Object>) condGroupObj;
    ArrayList<Integer> keyArray = (ArrayList<Integer>) ret.get(0);
    ArrayList<Object> rangeMatrix = (ArrayList<Object>) ret.get(1);

    keyArray.set(0, id1);
    keyArray.set(1, id2);
    ArrayList<Object> rangeArray = (ArrayList<Object>) rangeMatrix.get(0);
    ArrayList<Object> range = (ArrayList<Object>) rangeArray.get(0);
    range.set(0, start);
    range.set(1, end);
  }

  public Object toArray() {
    // Object[] rangeMatrix = new Object[this.range.size()];
    // for(int j = 0; j < this.range.size(); j ++)
    // {
    // Object[] rangeArray = new Object[this.range.get(j).size()];
    // for(int k = 0; k < this.range.get(j).size(); k ++)
    // {
    // rangeArray[k] = this.range.get(j).get(k).toArray();
    // }
    // rangeMatrix[j] = rangeArray;
    // }
    // Object[] ret = {this.key, rangeMatrix};

    List<Object> rangeMatrix = new ArrayList<Object>();
    for (int j = 0; j < this.range.size(); j++)
    {
      List<Object> rangeArray = new ArrayList<Object>();
      for (int k = 0; k < this.range.get(j).size(); k++)
      {
        rangeArray.add(this.range.get(j).get(k).toArray());
      }
      rangeMatrix.add(rangeArray);
    }

    List<Object> ret = new ArrayList<Object>();
    ret.add(this.key);
    ret.add(rangeMatrix);
    return ret;
  }

}
