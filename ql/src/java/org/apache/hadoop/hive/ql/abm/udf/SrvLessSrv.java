package org.apache.hadoop.hive.ql.abm.udf;


public class SrvLessSrv extends SrvCompareSrv {

  @Override
  protected void updateRet(int id1, int id2, double lower1, double lower2, double upper1,
      double upper2) {
    if (upper1 < lower2) {
      ret.update(id1, id2, Double.POSITIVE_INFINITY);
    } else {
      ret.update(id1, id2, 0);
    }
  }

  @Override
  protected String udfFuncName() {
    return "Srv_Less_Srv";
  }

}
