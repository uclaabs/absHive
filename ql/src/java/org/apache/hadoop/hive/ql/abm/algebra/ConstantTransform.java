package org.apache.hadoop.hive.ql.abm.algebra;


public class ConstantTransform extends Transform {

  private final Object val;

  public ConstantTransform(Object v) {
    val = v;
  }

  public Object getVal() {
    return val;
  }

}
