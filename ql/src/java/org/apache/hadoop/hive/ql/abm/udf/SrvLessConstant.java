package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;

public class SrvLessConstant extends SrvCompareConstant {

	@Override
  protected void updateRet(int id, double value)
	{
		CondGroup.update(this.ret, id, Double.NEGATIVE_INFINITY, value);
	}

}