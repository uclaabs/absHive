package org.apache.hadoop.hive.ql.abm.algebra;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public abstract class UnaryTransform extends Transform {

  private final AggregateInfo param;

  public UnaryTransform(AggregateInfo parameter) {
    param = parameter;
  }

  @Override
  public Set<AggregateInfo> getAggregatesInvolved() {
    Set<AggregateInfo> ret = new HashSet<AggregateInfo>();
    ret.add(param);
    return ret;
  }

}
