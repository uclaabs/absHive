package org.apache.hadoop.hive.ql.abm.algebra;

import java.util.Set;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public class ConstantTransform extends Transform {

  private final Object val;

  public ConstantTransform(Object v) {
    val = v;
  }

  @Override
  public Set<AggregateInfo> getAggregatesInvolved() {
    return new HashSet<AggregateInfo>();
  }

}
