package org.apache.hadoop.hive.ql.abm.udf;

public class SrvGreaterEqualFilter extends SrvCompareFilter {

  @Override
  protected void updateRet(double value, double lower, double upper) {
    ret.set(value <= upper);
  }

  @Override
  protected String udfFuncName() {
    return "Srv_Greater_Equal_Filter";
  }

}
