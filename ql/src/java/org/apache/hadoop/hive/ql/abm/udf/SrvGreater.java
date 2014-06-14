package org.apache.hadoop.hive.ql.abm.udf;


public class SrvGreater extends SrvCompare {

  @Override
  protected void updateRet(int id, double value, double lower, double upper) {
    if (value < lower) {
      ret.update(id, Double.NEGATIVE_INFINITY);
    } else {
      ret.update(id, value);
    }
  }

  @Override
  protected String udfFuncName() {
    return "Srv_Greater";
  }
}
