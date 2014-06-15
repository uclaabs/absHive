package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public class IdParser {

  private final IntObjectInspector oi;

  public IdParser(ObjectInspector oi) {
    this.oi = (IntObjectInspector) oi;
  }

  public int parse(Object o) {
    return oi.get(o);
  }

}
