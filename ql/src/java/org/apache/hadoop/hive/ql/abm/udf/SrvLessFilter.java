package org.apache.hadoop.hive.ql.abm.udf;

public class SrvLessFilter extends SrvCompareFilter {
  
  @Override
  protected boolean updateRet(double value, double lower, double upper)
  {
    if(value < lower) {
      return false;
    } else {
      return true;
    }
  }

}
