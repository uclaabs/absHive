package org.apache.hadoop.hive.ql.cs;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Order By Operator
 * @author victor
 *
 */
public class SExtractOperator extends SOperator {

	public SExtractOperator(ExtractOperator op, List<SOperator> parents, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		super(op, parents, ctx);
	}
	
	@Override
	public String prettyString() {
		return super.prettyString() + " Order By ";
	}
	
	@Override
	public boolean isComplex(boolean hasComplexOnTheWay) {
		for (SOperator p : parents) {
			if (p.isComplex(false || hasComplexOnTheWay)) {
				return true;
			}
		}
		
		return false;
	}
}