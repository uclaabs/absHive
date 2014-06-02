package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Collection;

public class KeyWrapper extends IntArrayList {

  private static final long serialVersionUID = 1L;

  public KeyWrapper() {
    super();
  }

  public KeyWrapper (Collection<? extends Integer> c) {
    super(c);
  }

  public KeyWrapper(final int capacity) {
    super(capacity);
  }

  /**
   * Return a copy of the key.
   * @return
   */
  public KeyWrapper copyKey() {
    return new KeyWrapper(this);
  }

}