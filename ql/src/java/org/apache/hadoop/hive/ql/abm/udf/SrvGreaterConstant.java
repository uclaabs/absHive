package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;


public class SrvGreaterConstant extends SrvCompareConstant {

  @Override
  protected void updateRet(int id, double value)
  {
    Condition.update(this.ret, id, value, Double.POSITIVE_INFINITY);
  }

}
