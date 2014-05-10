package org.apache.hadoop.hive.ql.abm.algebra;

import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;

public class IdentityTransform extends UnaryTransform {

  public IdentityTransform(AggregateInfo parameter) {
    super(parameter);
  }

}
