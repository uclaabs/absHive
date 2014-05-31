package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class RangeList extends DoubleArrayList {

  private static final long serialVersionUID = 1L;

  public double getLower(int i) {
    return getDouble(i << 1);
  }

  public double getUpper(int i) {
    return getDouble((i << 1) + 1);
  }

  @Override
  public int size() {
    return (super.size() >> 1);
  }

  public void addAll(Object o, ListObjectInspector loi) {
    DoubleObjectInspector eoi = (DoubleObjectInspector) loi.getListElementObjectInspector();
    int length = loi.getListLength(o);
    ensureCapacity(length);
    for (int i = 0; i < length; ++i) {
      add(eoi.get(loi.getListElement(o, i)));
    }
  }

}
