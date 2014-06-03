package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public abstract class Parser {

  protected ObjectInspector _oi;

  public Parser(ObjectInspector oi) {
    _oi = oi;
  }

  public ObjectInspector getObjectInspector() {
    return _oi;
  }

}
