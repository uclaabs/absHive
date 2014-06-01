package org.apache.hadoop.hive.ql.abm.algebra;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public abstract class UnaryTransform extends Transform {

  private final AggregateInfo param;

  public UnaryTransform(AggregateInfo parameter) {
    param = parameter;
    aggregates.add(param);
  }

}
