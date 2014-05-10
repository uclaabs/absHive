package org.apache.hadoop.hive.ql.abm.algebra;

import java.util.Set;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public abstract class BinaryTransform extends Transform {

  private final Transform lhs;
  private final Transform rhs;

  public BinaryTransform(Transform left, Transform right) {
    lhs = left;
    rhs = right;
  }

  @Override
  public Set<AggregateInfo> getAggregatesInvolved() {
    Set<AggregateInfo> ret = lhs.getAggregatesInvolved();
    ret.addAll(rhs.getAggregatesInvolved());
    return ret;
  }
}
