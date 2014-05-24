package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class KeyWrapper {

  private final IntArrayList keyBuf;

  public KeyWrapper() {
    keyBuf = new IntArrayList();
  }

  public void newKey() {
    keyBuf.clear();
  }

  public void addElement(int key) {
    keyBuf.add(key);
  }

  /**
   * Return a copy of the key.
   * @return
   */
  public IntArrayList copyKey() {
    return keyBuf.clone();
  }

  /**
   * Return the key kept internally.
   * @return
   */
  public IntArrayList getKey() {
    return keyBuf;
  }

}