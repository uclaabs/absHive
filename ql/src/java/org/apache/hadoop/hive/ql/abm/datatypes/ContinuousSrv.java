package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class ContinuousSrv extends DoubleArrayList {

  private static final long serialVersionUID = 1L;

  public ContinuousSrv(int capacity) {
    super(capacity);
  }

}
