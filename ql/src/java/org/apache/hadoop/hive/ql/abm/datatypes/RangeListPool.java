package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.Stack;

public class RangeListPool extends Stack<RangeList> {

  private static final long serialVersionUID = 1L;

  public RangeList borrow() {
    if (!isEmpty()) {
      return pop();
    }
    return new RangeList();
  }

  public void yield(RangeList o) {
    o.clear();
    this.push(o);
  }

}
