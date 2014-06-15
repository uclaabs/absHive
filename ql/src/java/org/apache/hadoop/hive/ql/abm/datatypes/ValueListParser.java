package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class ValueListParser {

  private ListObjectInspector oi = null;
  private DoubleObjectInspector eoi = null;

  public ValueListParser(ObjectInspector oi) {
    this.oi = (ListObjectInspector) oi;
    eoi = (DoubleObjectInspector) this.oi.getListElementObjectInspector();
  }

  public DoubleArrayList parse(Object o) {
    int length = oi.getListLength(o);
    DoubleArrayList ret = new DoubleArrayList(length);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
    return ret;
  }

  public void parseInto(Object o, DoubleArrayList ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
  }

}
