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

}
