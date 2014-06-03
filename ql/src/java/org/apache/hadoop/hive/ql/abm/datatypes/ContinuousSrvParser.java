package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class ContinuousSrvParser extends Parser {

  private final ListObjectInspector oi;
  private final DoubleObjectInspector eoi;

  public ContinuousSrvParser(ObjectInspector oi) {
    super(oi);
    this.oi = (ListObjectInspector) oi;
    eoi = (DoubleObjectInspector) this.oi.getListElementObjectInspector();
  }

  public ContinuousSrv parse(Object o) {
    int length = oi.getListLength(o);
    ContinuousSrv ret = new ContinuousSrv(length - 2);
    for (int i = 2; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
    return ret;
  }

}
