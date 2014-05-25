package org.apache.hadoop.hive.ql.abm.datatypes;


import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class Condition {

  private int[] ids;
  private final double[] range;

  public static List<String> columnName = Arrays.asList("ID","RANGE");

  final static ObjectInspector[] objectInspector={
    ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector),
    ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)};

  public static List<ObjectInspector> objectInspectorType=Arrays.asList(objectInspector);

  public static final StructObjectInspector conditionInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnName, objectInspectorType);
  public static final ListObjectInspector idListInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  public static final ListObjectInspector rangeListInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
  public static final IntObjectInspector idInspector = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  public static final DoubleObjectInspector rangeInspector = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

  public Condition(int id, double start, double end)
  {
    ids = new int[]{id};
    range = new double[]{start, end};
  }

  public Condition(boolean singleFlag)
  {
    if(singleFlag) {
      ids = new int[1];
    } else {
      ids = new int[2];
    }

    range = new double[2];
  }

  public Condition(Object obj)
  {
    Object IDObj = conditionInspector.getStructFieldData(obj, conditionInspector.getStructFieldRef("ID"));
    Object rangeObj = conditionInspector.getStructFieldData(obj, conditionInspector.getStructFieldRef("RANGE"));

    int IDSize = idListInspector.getListLength(IDObj);
    ids = new int[IDSize];
    for(int i = 0; i < IDSize; i ++) {
      ids[i] = (Integer) idListInspector.getListElement(IDObj, i);
    }

    range = new double[2];
    range[0] = (Double) rangeListInspector.getListElement(rangeObj, 0);
    range[1] = (Double) rangeListInspector.getListElement(rangeObj, 1);

  }

  public static void update(Object obj, int id, double start, double end)
  {
    Object[] res = (Object[]) obj;
    Object[] idObj = (Object[]) res[0];
    Object[] rangeObj = (Object[]) res[1];
    idObj[0] = id;
    rangeObj[0] = start;
    rangeObj[1] = end;
  }

  public static void update(Object obj, int id0, int id1, double start, double end)
  {
    Object[] res = (Object[]) obj;
    Object[] idObj = (Object[]) res[0];
    Object[] rangeObj = (Object[]) res[1];
    idObj[0] = id0;
    idObj[1] = id1;
    rangeObj[0] = start;
    rangeObj[1] = end;
  }

  public static Condition getCondition(LazyBinaryStruct lazyCond)
  {
    Condition con = null;
    LazyBinaryArray idObj = (LazyBinaryArray) lazyCond.getField(0);
    LazyBinaryArray rangeObj = (LazyBinaryArray) lazyCond.getField(1);

    if(idObj.getListLength() == 1)
    {
      con = new Condition(true);
      con.setID(((IntWritable)idObj.getListElementObject(0)).get());
    }
    else
    {
      con = new Condition(false);
      con.setID(((IntWritable)idObj.getListElementObject(0)).get(), ((IntWritable)idObj.getListElementObject(1)).get());
    }

    con.setRange(((DoubleWritable)rangeObj.getListElementObject(0)).get(), ((DoubleWritable)rangeObj.getListElementObject(1)).get());

    return con;
  }

  public void setID(int id)
  {
    ids[0] = id;
  }

  public void setID(int id1, int id2)
  {
    ids[0] = id1;
    ids[1] = id2;
  }

  public void setRange(double start, double end)
  {
    range[0] = start;
    range[1] = end;
  }

  public Object[] IntArrayToObjArray(int[] darray)
  {
    Object[] res = new Object[darray.length];
    for(int i = 0; i < darray.length; i ++) {
      res[i] = darray[i];
    }
    return res;
  }

  public Object toArray()
  {
    Object[] idObj = IntArrayToObjArray(this.ids);
    Object[] rangeObj = {this.range[0], this.range[1]};
    Object[] res = {idObj, rangeObj};
    return res;
  }

  public int getId()
  {
    return this.ids[0];
  }

  public double[] getRange()
  {
    return this.range;
  }

  public boolean getFlag() {
    if(this.range[0] == Double.NEGATIVE_INFINITY) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public String toString()
  {
    return "(" + this.ids[0] + "--" + this.range[0] + "," + this.range[1] + ")";
  }

}