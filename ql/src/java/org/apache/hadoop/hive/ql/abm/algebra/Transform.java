package org.apache.hadoop.hive.ql.abm.algebra;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public abstract class Transform {

  protected List<AggregateInfo> aggregates = new ArrayList<AggregateInfo>();

  public List<AggregateInfo> getAggregatesInvolved() {
    return aggregates;
  }

}
