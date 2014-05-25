package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;


public class SrvGreaterConstant extends SrvCompareConstant {

  @Override
  protected void updateRet(int id, double value)
  {
    CondGroup.update(this.ret, id, value, Double.POSITIVE_INFINITY);
  }

}
