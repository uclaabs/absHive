package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class KeyWrapper extends IntArrayList {

  private static final long serialVersionUID = 1L;

  public void newKey() {
    clear();
  }

  /**
   * Return a copy of the key.
   * @return
   */
  public IntArrayList copyKey() {
    return clone();
  }

}