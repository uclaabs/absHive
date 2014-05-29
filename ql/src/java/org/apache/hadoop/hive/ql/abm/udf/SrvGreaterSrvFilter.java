package org.apache.hadoop.hive.ql.abm.udf;

public class SrvGreaterSrvFilter extends SrvCompareSrvFilter {
  
  @Override
  protected boolean updateRet( double lower1, double lower2, double upper1, double upper2)
  {
    if(upper1 < lower2)
      return false;
    else
      return true;
  }

}
