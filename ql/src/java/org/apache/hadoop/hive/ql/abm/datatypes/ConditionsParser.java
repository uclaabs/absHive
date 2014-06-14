package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

public class ConditionsParser extends Parser {

  private final BinaryObjectInspector oi;

  public ConditionsParser(ObjectInspector oi) {
    super(oi);
    this.oi = (BinaryObjectInspector) oi;
  }

  public Conditions parse(Object o) {
    byte[] buf = oi.getPrimitiveWritableObject(o).getBytes();
    return ConditionIO.deserialize(buf);
  }

}
