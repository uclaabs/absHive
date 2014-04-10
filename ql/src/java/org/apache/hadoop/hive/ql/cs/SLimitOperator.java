package org.apache.hadoop.hive.ql.cs;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class SLimitOperator extends SOperator {

	int limit;

	public SLimitOperator(LimitOperator op, List<SOperator> parents, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		super(op, parents, ctx);
		limit = op.getConf().getLimit();
	}
	

	@Override
	public String prettyString() {
		return super.prettyString() + " Limit: " + limit;
	}
}