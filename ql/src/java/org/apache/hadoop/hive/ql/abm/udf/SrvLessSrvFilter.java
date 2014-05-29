package org.apache.hadoop.hive.ql.abm.udf;

public class SrvLessSrvFilter extends SrvCompareSrvFilter {
  
  @Override
  protected boolean updateRet( double lower1, double lower2, double upper1, double upper2)
  {
    if(lower1 > upper2)
      return false;
    else
      return true;
  }

}
