package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class KeyWrapper {
  private final IntArrayList lst;

  public KeyWrapper() {
    lst = new IntArrayList();
  }

  public void newKey() {
    lst.clear();
  }

  public void addKey(Integer key) {
    lst.add(key);
  }

  /**
   * return a copy of internal lst
   * @return
   */
  public IntArrayList copyKey() {
    return lst.clone();
  }

  /**
   * return the internal lst
   * @return
   */
  public IntArrayList getKey() {
    return lst;
  }
}