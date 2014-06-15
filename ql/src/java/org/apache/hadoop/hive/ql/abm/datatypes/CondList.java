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

  private final KeyWrapper keyList = new KeyWrapper();
  private final List<RangeList> rangeMatrix = new ArrayList<RangeList>();
  private final Object[] ret = {keyList, rangeMatrix};

  public final static ListObjectInspector intListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  public final static ListObjectInspector doubleMatrixOI = ObjectInspectorFactory
      .getStandardListObjectInspector(
      ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
  public final static List<String> columnName = new ArrayList<String>(Arrays.asList("Keys", "Ranges"));
  public final static List<ObjectInspector> objectInspectorType =
      new ArrayList<ObjectInspector>(Arrays.asList(
          (ObjectInspector) intListOI, (ObjectInspector) doubleMatrixOI));
  public final static StructObjectInspector condListOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);

  public void update(int id, double value) {
    keyList.set(0, id);
    rangeMatrix.get(0).set(0, value);
  }

  public void update(int id1, int id2, double value) {
    keyList.set(0, id1);
    keyList.set(1, id2);
    rangeMatrix.get(0).set(0, value);
  }

  public void addKey(int key) {
    keyList.add(key);
  }

  public void addRangeValue(double value) {
    RangeList newlist = new RangeList();
    newlist.add(value);
    rangeMatrix.add(newlist);
  }

  public void addKeys(KeyWrapper newKeys) {
    keyList.addAll(newKeys);
  }

  public void addRanges(RangeList newRanges) {
    rangeMatrix.add(newRanges);
  }

  public void addRange(double[] rangeArray) {
    for (int i = 0; i < rangeArray.length; i++) {
      this.rangeMatrix.get(i).add(rangeArray[i]);
    }
  }

  public void clear() {
    keyList.clear();
    rangeMatrix.clear();
  }

  public Object toArray() {
    return ret;
  }

}
