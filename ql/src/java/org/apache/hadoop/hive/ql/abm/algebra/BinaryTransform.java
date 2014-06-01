package org.apache.hadoop.hive.ql.abm.algebra;


public abstract class BinaryTransform extends Transform {

  protected final Transform lhs;
  protected final Transform rhs;

  public BinaryTransform(Transform left, Transform right) {
    lhs = left;
    rhs = right;
    aggregates.addAll(lhs.getAggregatesInvolved());
    aggregates.addAll(rhs.getAggregatesInvolved());
  }

}
