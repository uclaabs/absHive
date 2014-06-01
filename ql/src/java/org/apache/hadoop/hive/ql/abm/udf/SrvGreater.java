package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;

public class SrvGreater extends SrvCompare {

  @Override
  protected void updateRet(long id, double value, double lower, double upper)
  {
    if(value < lower) {
      CondList.update(this.ret, id, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    } else {
      CondList.update(this.ret, id, value, Double.POSITIVE_INFINITY);
    }
    
  }
}
