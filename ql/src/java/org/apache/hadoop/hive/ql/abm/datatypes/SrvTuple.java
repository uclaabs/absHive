package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Serializable;
import java.util.List;

public class SrvTuple implements Serializable {

  private static final long serialVersionUID = 1L;

  public final double[] srv;
  public final IntArrayList key;
  public final List<RangeList> range;

  public SrvTuple(double[] srv, IntArrayList key, List<RangeList> range) {
    this.srv = srv;
    this.key = key;
    this.range = range;
  }

}
