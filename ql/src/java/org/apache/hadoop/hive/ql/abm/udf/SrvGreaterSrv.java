package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;

public class SrvGreaterSrv extends SrvCompareSrv {
  
  protected void updateRet(int id1, int id2, double lower1, double lower2, double upper1, double upper2)
  {
    if(lower1 < upper2) {
      CondList.update(this.ret, id1, id2, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    } else {
      CondList.update(this.ret, id1, id2, 0, Double.POSITIVE_INFINITY);
    }
  }

}
