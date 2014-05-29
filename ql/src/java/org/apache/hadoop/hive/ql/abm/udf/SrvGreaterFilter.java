package org.apache.hadoop.hive.ql.abm.udf;

public class SrvGreaterFilter extends SrvCompareFilter {

  @Override
  protected boolean updateRet(double value, double lower, double upper)
  {
    if(value > upper) {
      return false;
    } else {
      return true;
    }
  }
  
}
