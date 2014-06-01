package org.apache.hadoop.hive.ql.abm.algebra;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public abstract class Transform {

  protected Set<AggregateInfo> aggregates = new HashSet<AggregateInfo>();

  public Set<AggregateInfo> getAggregatesInvolved() {
    return aggregates;
  }

}
