package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public class KeyWrapper extends IntArrayList {

  private static final long serialVersionUID = 1L;

  public void parseKey(Object o, ObjectInspector oi) {
    clear();
    ListObjectInspector loi = (ListObjectInspector) oi;
    IntObjectInspector eoi = (IntObjectInspector) loi.getListElementObjectInspector();
    int length = loi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      add(eoi.get(loi.getListElement(o, i)));
    }
  }

  /**
   * Return a copy of the key.
   * @return
   */
  public KeyWrapper copyKey() {
    return (KeyWrapper) clone();
  }

}