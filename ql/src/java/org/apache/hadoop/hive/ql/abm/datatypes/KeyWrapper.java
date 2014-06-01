package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Collection;

public class KeyWrapper extends LongArrayList {

  private static final long serialVersionUID = 1L;
  
  public KeyWrapper() {
    super();
  }
  
  public KeyWrapper (Collection<? extends Long> c) {
    super(c);
  }
  
  public KeyWrapper(final int capacity) {
    super(capacity);
  }
  
  /**
   * Return a copy of the key.
   * @return
   */
//  public KeyWrapper copyKey() {
//    return new KeyWrapper();
//  }
  public KeyWrapper copyKey() {
    return new KeyWrapper(this);
  }

}