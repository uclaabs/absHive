package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;

public class DataUtils {

  public static DoubleArrayList parseDoubleArray(Object o, ObjectInspector oi) {
    DoubleArrayList ret =  new DoubleArrayList();
    parseDoubleArray(o, oi, ret);
    return ret;
  }

  public static void parseDoubleArray(Object o, ObjectInspector oi, DoubleArrayList output) {
    ListObjectInspector loi = (ListObjectInspector) oi;
    DoubleObjectInspector eoi = (DoubleObjectInspector) loi.getListElementObjectInspector();
    int length = loi.getListLength(o);
    output.ensureCapacity(length);
    for (int i = 0; i < length; ++i) {
      output.add(eoi.get(loi.getListElement(o, i)));
    }
  }

  public static LongArrayList parseLongArray(Object o, ObjectInspector oi) {
    LongArrayList ret =  new LongArrayList();
    parseLongArray(o, oi, ret);
    return ret;
  }

  public static void parseLongArray(Object o, ObjectInspector oi, LongArrayList output) {
    ListObjectInspector loi = (ListObjectInspector) oi;
    LongObjectInspector eoi = (LongObjectInspector) loi.getListElementObjectInspector();
    int length = loi.getListLength(o);
    output.ensureCapacity(length);
    for (int i = 0; i < length; ++i) {
      output.add(eoi.get(loi.getListElement(o, i)));
    }
  }

}
