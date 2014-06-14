package org.apache.hadoop.hive.ql.abm.udf;

public class SrvLessEqualFilter extends SrvCompareFilter {
  
  @Override
  protected void updateRet(double value, double lower, double upper) {
    ret.set(value > lower);
  }

  @Override
  protected String udfFuncName() {
    return "Srv_Less_Equal_Filter";
  }

}
