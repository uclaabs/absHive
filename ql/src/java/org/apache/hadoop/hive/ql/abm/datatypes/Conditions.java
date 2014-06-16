package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Serializable;
import java.util.List;

public class Conditions implements Serializable {

  private static final long serialVersionUID = 1L;

  public final IntArrayList keys;
  public final List<RangeList> ranges;

  public Conditions(IntArrayList keys, List<RangeList> ranges) {
    this.keys = keys;
    this.ranges = ranges;
  }

  public IntArrayList getKeys() {
    return keys;
  }

  public List<RangeList> getRanges() {
    return ranges;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    builder.append(keys.toString());
    builder.append(ranges.toString());
    builder.append('}');
    return builder.toString();
  }

}
