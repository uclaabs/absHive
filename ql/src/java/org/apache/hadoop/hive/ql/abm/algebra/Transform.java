package org.apache.hadoop.hive.ql.abm.algebra;

import java.util.Set;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public abstract class Transform {

  public abstract Set<AggregateInfo> getAggregatesInvolved();

}
