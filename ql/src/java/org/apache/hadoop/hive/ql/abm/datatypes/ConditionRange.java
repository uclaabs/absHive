package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ConditionRange {

  private double start;
  private double end;

  public static List<String> columnNames = Arrays.asList("Start", "End");
  public static ObjectInspector[] objectInspectors = {PrimitiveObjectInspectorFactory.javaDoubleObjectInspector, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector};
  public static List<ObjectInspector> objectInspectorType=Arrays.asList(objectInspectors);
  public static DoubleObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  public static StructObjectInspector conditionRangeInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, objectInspectorType);

  public ConditionRange(Object rangeObj)
  {
    this.start = doubleOI.get(conditionRangeInspector.getStructFieldData(rangeObj, conditionRangeInspector.getStructFieldRef("Start")));
    this.end = doubleOI.get(conditionRangeInspector.getStructFieldData(rangeObj, conditionRangeInspector.getStructFieldRef("End")));
  }

  public ConditionRange(LazyBinaryStruct rangeBinaryStruct)
  {
    this.start = ((DoubleWritable)rangeBinaryStruct.getField(0)).get();
    this.end = ((DoubleWritable)rangeBinaryStruct.getField(1)).get();
  }

  public ConditionRange(double left, double right)
  {
    this.start = left;
    this.end = right;
  }

  public void setStart(double value)
  {
    this.start = value;
  }

  public void setEnd(double value)
  {
    this.end = value;
  }

  public double getStart()
  {
    return this.start;
  }

  public double getEnd()
  {
    return this.end;
  }

  public double getValue(boolean flag)
  {
    if(flag) {
      return this.start;
    } else {
      return this.end;
    }
  }

  public boolean getFlag()
  {
    if(start == Double.NEGATIVE_INFINITY) {
      return false;
    } else {
      return true;
    }
  }

  public Object toArray()
  {
//    Object[] ret = {this.start, this.end};
    List<Object> ret = new ArrayList<Object>();
    ret.add(this.start);
    ret.add(this.end);
    return ret;
  }

  @Override
  public String toString()
  {
    return "(" + start + "\t" + end + ")";
  }

  @Override
  public ConditionRange clone()
  {
    return new ConditionRange(this.start, this.end);
  }

  public static boolean isBase(Object rangeObj) {
    double start = doubleOI.get(conditionRangeInspector.getStructFieldData(rangeObj, conditionRangeInspector.getStructFieldRef("Start")));
    double end = doubleOI.get(conditionRangeInspector.getStructFieldData(rangeObj, conditionRangeInspector.getStructFieldRef("End")));

    if(start == Double.NEGATIVE_INFINITY && end == Double.POSITIVE_INFINITY) {
      return true;
    } else {
      return false;
    }
  }

}
