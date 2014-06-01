package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public abstract class Parser {
  
  protected ObjectInspector oi;
  
  public Parser(ObjectInspector oi) {
    this.oi = oi;
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }
  
}
