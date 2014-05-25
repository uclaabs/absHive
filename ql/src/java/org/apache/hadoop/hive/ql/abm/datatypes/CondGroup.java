package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CondGroup 
{
  int cnt;
  List<List<Integer>> keys;
  List<List<List<ConditionRange>>> ranges;

  public static List<String> columnNames = Arrays.asList("Keys", "Values");

  public static ObjectInspector[] objectInspectors = {ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector)),
    ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(ConditionRange.conditionRangeInspector)))};

  public static List<ObjectInspector> objectInspectorType=Arrays.asList(objectInspectors);

  public static StructObjectInspector condGroupInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, objectInspectorType);

  public CondGroup()
  {
    cnt = -1;
    this.keys = new ArrayList<List<Integer>>();
    this.ranges = new ArrayList<List<List<ConditionRange>>>();
  }
  
  public void addGroup(IntArrayList keyList)
  {
    cnt ++;
    List<Integer> keyArray = new ArrayList<Integer>(keyList);
    this.keys.add(keyArray);
    List<List<ConditionRange>> rangeMatrix  = new ArrayList<List<ConditionRange>>();
    for(int i = 0; i < keyList.size(); i ++)
      rangeMatrix.add(new ArrayList<ConditionRange>());
    this.ranges.add(rangeMatrix);
  }
  
  public List<List<ConditionRange>> getRangeMatrix()
  {
    return this.ranges.get(cnt);
  }

  public void initialize()
  {
    // create a condition with one key and one range

    // add key
    List<Integer> theKey = new ArrayList<Integer>(1);
    theKey.add(-1);
    this.keys.add(theKey);

    // add range
    List<List<ConditionRange>> rangeMatrix = new ArrayList<List<ConditionRange>>(1);
    List<ConditionRange> rangeArray = new ArrayList<ConditionRange>(1);
    rangeArray.add(new ConditionRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    rangeMatrix.add(rangeArray);
    this.ranges.add(rangeMatrix);
  }
  
  public static void update(Object condGroupObj, int id, double start, double end)
  {
    Object[] ret = (Object[])condGroupObj;
    Object[] keyObjs = (Object[])ret[0];
    Object[] rangeObjs = (Object[])ret[1];
    
    Object[] keyArray = (Object[])keyObjs[0];
    keyArray[0] = id;
    Object[] rangeMatrix = (Object[]) rangeObjs[0];
    Object[] rangeArray = (Object[]) rangeMatrix[0];
    Object[] range = (Object[])rangeArray[0];
    range[0] = start;
    range[1] = end;
  }

  public Object toArray()
  {
    Object[] keyObjs = new Object[this.keys.size()];
    for(int i = 0; i < this.keys.size(); i ++) 
      keyObjs[i] = this.keys.get(i).toArray();
    
    Object[] rangeObjs = new Object[this.ranges.size()];
    for(int i = 0; i < this.ranges.size(); i ++)
    {
      Object[] rangeMatrix = new Object[this.ranges.get(i).size()];
      for(int j = 0; j < this.ranges.get(i).size(); j ++)
      {
        Object[] rangeArray = new Object[this.ranges.get(i).get(j).size()];
        for(int k = 0; k < this.ranges.get(i).get(j).size(); k ++)
        {
           rangeArray[k] = this.ranges.get(i).get(j).get(k).toArray();
        }
        rangeMatrix[j] = rangeArray;
      }
      rangeObjs[i] = rangeMatrix;
    }

    Object[] ret = {keyObjs, rangeObjs};
    return ret;
  }

}
