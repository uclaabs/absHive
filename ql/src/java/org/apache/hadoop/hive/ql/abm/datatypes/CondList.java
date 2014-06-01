package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CondList {

  KeyWrapper keyList;
  List<RangeList> rangeMatrix;
  Object[] ret;
  List<Object> rangeRet;

  public static ListObjectInspector longListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
  public static ListObjectInspector doubleMatrixOI = ObjectInspectorFactory
      .getStandardListObjectInspector(
      ObjectInspectorFactory
          .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)); // TODO
  public static List<String> columnName = Arrays.asList("Keys", "Ranges");
  public static List<ObjectInspector> objectInspectorType = Arrays.asList(
      (ObjectInspector) longListOI, (ObjectInspector) doubleMatrixOI);
  public static StructObjectInspector condListOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);

  public CondList() {
    keyList = new KeyWrapper();
    rangeMatrix = new ArrayList<RangeList>();
    ret = new Object[2];
    rangeRet = new ArrayList<Object>();
  }

  @SuppressWarnings("unchecked")
  public static void update(Object condListObj, long id, double start, double end) {
    Object[] ret = (Object[]) condListObj;
    KeyWrapper keyArray = (KeyWrapper) ret[0];
    ArrayList<Object> rangeMatrix = (ArrayList<Object>) ret[1];

    keyArray.set(0, id);
    RangeList rangeArray = (RangeList) rangeMatrix.get(0);
    rangeArray.set(0, start);
    rangeArray.set(1, end);
  }

  @SuppressWarnings("unchecked")
  public static void update(Object condListObj, long id1, long id2, double start, double end) {
    Object[] ret = (Object[]) condListObj;
    KeyWrapper keyArray = (KeyWrapper) ret[0];
    ArrayList<Object> rangeMatrix = (ArrayList<Object>) ret[1];

    keyArray.set(0, id1);
    keyArray.set(1, id2);
    RangeList rangeArray = (RangeList) rangeMatrix.get(0);
    rangeArray.set(0, start);
    rangeArray.set(1, end);
  }

  public void addKey(long key) {
    this.keyList.add(key);
  }

  public void addPairRange(double lower, double upper) {
    RangeList newlist = new RangeList();
    newlist.add(lower);
    newlist.add(upper);
    this.rangeMatrix.add(newlist);
  }

  public void addKeys(KeyWrapper newKeys) {
    this.keyList.addAll(newKeys);
  }

  public void addRanges(RangeList newRanges) {
    this.rangeMatrix.add(newRanges);
  }

  public void addRange(double[] rangeArray) {
    for (int i = 0; i < rangeArray.length / 2; i++) {
      this.rangeMatrix.get(i).add(rangeArray[2 * i]);
      this.rangeMatrix.get(i).add(rangeArray[2 * i + 1]);
    }
  }

  public void clear() {
    this.keyList.clear();
    this.rangeMatrix.clear();
  }

  public Object toArray() {
    ret[0] = keyList;
    ret[1] = rangeMatrix;
    return ret;
  }

}
