package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;

public class SrvLess extends SrvCompare {

  @Override
  protected void updateRet(int id, double value, double lower, double upper)
  {
    if(value > upper) {
      CondGroup.update(this.ret, id, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    } else {
      CondGroup.update(this.ret, id, Double.NEGATIVE_INFINITY, value);
    }
    
  }
}
