package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;

public class TupleMap extends Int2ReferenceOpenHashMap<SrvTuple> {

  private static final long serialVersionUID = 1L;

}

class FakeTupleMap extends TupleMap {

  private static final long serialVersionUID = 1L;

  private SrvTuple defaultTuple = null;

  @Override
  public SrvTuple get(int key) {
    return defaultTuple;
  }

  public void setDefaultTuple(SrvTuple defaultTuple) {
    this.defaultTuple = defaultTuple;
  }

}
