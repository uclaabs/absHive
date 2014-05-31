package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class RangeIO {

  public static double getLower(DoubleArrayList buf, int i) {
    return buf.getDouble(i << 1);
  }

  public static double getUpper(DoubleArrayList buf, int i) {
    return buf.getDouble((i << 1) + 1);
  }

}
