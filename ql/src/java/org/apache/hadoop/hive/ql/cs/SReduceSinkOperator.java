package org.apache.hadoop.hive.ql.cs;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

/**
 * ReduceSinkOperator may have constant ExprNodeDesc
 *
 * @author victor
 *
 */
public class SReduceSinkOperator extends SOperator {

	ReduceSinkDesc reduceSinkDesc;

	public SReduceSinkOperator(ReduceSinkOperator op, List<SOperator> parents, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		super(op, parents, ctx);
		reduceSinkDesc = ((ReduceSinkOperator)op).getConf();
	}

	@Override
  public String prettyString() {
		return super.prettyString() + " Key Cols: " + reduceSinkDesc.getKeyCols() + " Value Cols: " + reduceSinkDesc.getValueCols()
				+ " Partitioned Cols: " + reduceSinkDesc.getPartitionCols();
	}

	@Override
  public String toString() {
		return super.toString();
//		return super.toString() + "Key Cols: " + desc.getKeyCols() + "Value Cols: " + desc.getValueCols()
//				+ "Partitioned Cols: " + desc.getPartitionCols();
	}

	@Override
	public boolean isComplex(boolean hasComplexOnTheWay) {
		boolean flag = false;

		for (SDerivedColumn c : columns) {
			if (!(c instanceof SColumn)) {
				flag = true;
			}
		}

		for (SOperator p : parents) {
			if (p.isComplex(flag || hasComplexOnTheWay)) {
				return true;
			}
		}

		return false;
	}
}