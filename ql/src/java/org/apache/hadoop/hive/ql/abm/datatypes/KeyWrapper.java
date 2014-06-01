package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;

public class KeyWrapper extends LongArrayList {

  private static final long serialVersionUID = 1L;
  
  public void parseKey(Object keyObj, ListObjectInspector keyOI, LongObjectInspector keyValueOI) {
    clear();
    for(int j = 0; j < keyOI.getListLength(keyObj); j ++) {
      add(keyValueOI.get(keyOI.getListElement(keyObj, j)));
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