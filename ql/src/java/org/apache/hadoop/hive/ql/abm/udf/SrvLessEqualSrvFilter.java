package org.apache.hadoop.hive.ql.abm.udf;

public class SrvLessEqualSrvFilter extends SrvCompareSrvFilter {

  @Override
  protected void updateRet(double lower1, double lower2, double upper1, double upper2) {
    ret.set(lower1 <= upper2);
  }

  @Override
  protected String udfFuncName() {
    return "Srv_Less_Equal_Srv_Filter";
  }

}
