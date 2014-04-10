package org.apache.hadoop.hive.ql.cs;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class SFilterOperator extends SOperator {

	ExprNodeDesc expr;

	public SFilterOperator(FilterOperator op, List<SOperator> parents, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		super(op, parents, ctx);
		expr = (op).getConf().getPredicate();
	}

	public ExprNodeDesc getExpr() {
		return expr;
	}

	@Override
	public String prettyString() {
		if (expr != null) {
			return super.prettyString() + " Filter: " + expr.getExprString();
		}
		else {
			return super.prettyString() + "Filter: null";
		}
	}
	
//	@Override
//	public boolean isEligible(HashSet<FD> rules, HashSet<SBaseColumn> bases) {
//		// add FD
//		// TODO
//		
//		return true;
//	}
}