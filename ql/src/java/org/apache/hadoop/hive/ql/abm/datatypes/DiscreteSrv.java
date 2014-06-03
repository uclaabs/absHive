package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class DiscreteSrv extends DoubleArrayList {

  private static final long serialVersionUID = 1L;

  public DiscreteSrv(int capacity) {
    super(capacity);
  }

}
