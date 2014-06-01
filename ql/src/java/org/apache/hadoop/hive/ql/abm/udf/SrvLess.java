package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;

public class SrvLess extends SrvCompare {

  @Override
  protected void updateRet(long id, double value, double lower, double upper)
  {
    if(value > upper) {
      CondList.update(this.ret, id, Double.POSITIVE_INFINITY);
    } else {
      CondList.update(this.ret, id, value);
    }
    
  }
}
