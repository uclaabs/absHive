package org.apache.hadoop.hive.ql.abm.udf;

public class SrvGreaterSrvFilter extends SrvCompareSrvFilter {

  @Override
  protected void updateRet( double lower1, double lower2, double upper1, double upper2) {
    ret.set(upper1 >= lower2);
  }

}
