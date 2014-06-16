package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public class KeyWrapperParser {

  private ListObjectInspector oi = null;
  private IntObjectInspector eoi = null;

  public KeyWrapperParser(ObjectInspector oi) {
    this.oi = (ListObjectInspector) oi;
    eoi = (IntObjectInspector) this.oi.getListElementObjectInspector();
  }

  public KeyWrapper parse(Object o) {
    int length = oi.getListLength(o);
    KeyWrapper ret = new KeyWrapper(length);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
    return ret;
  }

  public void parseInto(Object o, KeyWrapper ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
  }

}
