package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class DiscreteSrv extends DoubleArrayList implements Srv {

  private static final long serialVersionUID = 1L;

  public DiscreteSrv(int capacity) {
    super(capacity);
  }

  @Override
  public double getMean(int index) {
    return get(index);
  }

  @Override
  public double getVar(int index) {
    return 0;
  }

}
