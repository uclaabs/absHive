package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;

public class SrvLessConstant extends SrvCompareConstant {

	@Override
  protected void updateRet(long id, double value)
	{
		CondList.update(this.ret, id, value);
	}

}