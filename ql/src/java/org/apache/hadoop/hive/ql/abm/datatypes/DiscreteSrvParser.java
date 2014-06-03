package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class DiscreteSrvParser extends Parser {

  private final ListObjectInspector oi;
  private final DoubleObjectInspector eoi;

  public DiscreteSrvParser(ObjectInspector oi) {
    super(oi);
    this.oi = (ListObjectInspector) oi;
    eoi = (DoubleObjectInspector) this.oi.getListElementObjectInspector();
  }

  public DiscreteSrv parse(Object o) {
    int length = oi.getListLength(o);
    DiscreteSrv ret = new DiscreteSrv(length - 2);
    for (int i = 2; i < length; ++i) {
      ret.add(eoi.get(oi.getListElement(o, i)));
    }
    return ret;
  }

}
