package org.apache.hadoop.hive.ql.abm.udf;

public class SrvLessEqual extends SrvCompare {
  
  @Override
  protected void updateRet(int id, double value, double lower, double upper) {
    if (value >= upper) {
      ret.update(id, Double.POSITIVE_INFINITY);
    } else {
      ret.update(id, value);
    }
  }

  @Override
  protected String udfFuncName() {
    return "Srv_Less_Equal";
  }

}
