package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class RangeListParser {

  private ListObjectInspector oi = null;
  private DoubleObjectInspector eoi = null;

  public RangeListParser(ObjectInspector oi) {
    this.oi = (ListObjectInspector) oi;
    eoi = (DoubleObjectInspector) this.oi.getListElementObjectInspector();
  }

  public RangeList parse(Object o) {
    int length = oi.getListLength(o);
    RangeList ret = new RangeList(length);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
    return ret;
  }

  public void parseInto(Object o, RangeList ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
  }

  public boolean isBase(Object o) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      double e = eoi.get(oi.getListElement(o, i));
      if (e != Double.NEGATIVE_INFINITY && e != Double.POSITIVE_INFINITY) {
        return false;
      }
    }
    return true;
  }

}
