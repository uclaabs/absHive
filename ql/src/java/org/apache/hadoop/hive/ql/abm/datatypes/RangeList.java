package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class RangeList extends DoubleArrayList {

  private static final long serialVersionUID = 1L;

  public RangeList() {
    super();
  }

  public RangeList(final int capacity) {
    super(capacity);
  }

  public boolean getFlag() {
    int i = 0;
    while (getLower(i) == Double.NEGATIVE_INFINITY && getUpper(i) == Double.POSITIVE_INFINITY) {
      i++;
    }
    if (getLower(i) == Double.NEGATIVE_INFINITY) {
      return false;
    } else {
      return true;
    }
  }

  public double getLower(int i) {
    return getDouble(i << 1);
  }

  public double getUpper(int i) {
    return getDouble((i << 1) + 1);
  }

  public int numCases() {
    return (super.size() >> 1);
  }

}
