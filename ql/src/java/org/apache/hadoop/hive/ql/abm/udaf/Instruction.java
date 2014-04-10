package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class Instruction {
  private final IntArrayList base = new IntArrayList();
  private final IntArrayList delta = new IntArrayList();
  private int size;

  public void add(int baseIndex, int deltaIndex) {
    if (size == base.size()) {
      base.add(baseIndex);
      delta.add(deltaIndex);
    } else {
      base.set(size, baseIndex);
      delta.set(size, deltaIndex);
    }

    size++;
  }

  public int getBase(int index) {
    assert index < size;
    return base.getInt(index);
  }

  public int getDelta(int index) {
    assert index < size;
    return delta.getInt(index);
  }

  public int size() {
    return size;
  }

  public void reset() {
    size = 0;
  }

}
