package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Serializable;
import java.util.List;

public class SrvTuple implements Serializable {

  private static final long serialVersionUID = 1L;

  public final double[] srv;
  public IntArrayList key;
  public List<RangeList> range;
  private transient IntArrayList idx = null;

  public SrvTuple(double[] srv, IntArrayList key, List<RangeList> range) {
    this.srv = srv;
    this.key = key;
    this.range = range;
  }

  public IntArrayList getIdx() {
    if (idx == null) {
      idx = new IntArrayList();
    }
    return idx;
  }

}
