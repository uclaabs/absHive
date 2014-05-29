package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;

public class SrvLessSrv extends SrvCompareSrv {
  
  protected void updateRet(int id1, int id2, double lower1, double lower2, double upper1, double upper2)
  {
    if(upper1 < lower2) {
      CondGroup.update(this.ret, id1, id2, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    } else {
      CondGroup.update(this.ret, id1, id2, Double.NEGATIVE_INFINITY, 0);
    }
  }

}
