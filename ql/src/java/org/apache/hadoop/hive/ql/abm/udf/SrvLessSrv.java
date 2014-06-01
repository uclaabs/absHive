package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;

public class SrvLessSrv extends SrvCompareSrv {
  
  protected void updateRet(int id1, int id2, double lower1, double lower2, double upper1, double upper2)
  {
    if(upper1 < lower2) {
      CondList.update(this.ret, id1, id2, Double.POSITIVE_INFINITY);
    } else {
      CondList.update(this.ret, id1, id2, 0);
    }
  }

}
