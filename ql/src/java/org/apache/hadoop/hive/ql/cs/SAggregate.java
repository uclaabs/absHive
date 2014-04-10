package org.apache.hadoop.hive.ql.cs;

import java.util.HashSet;

import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

public class SAggregate extends SDerivedColumn {

	AggregationDesc desc;

	public SAggregate(String name, String tableAlias, SOperator sop, AggregationDesc desc) {
		super(name, tableAlias, sop);
		this.desc = desc;
	}

	@Override
	public boolean isGeneratedByAggregate() {
		return true;
	}

	@Override
	public boolean isCorrelatedWithAggregate() {
		return true;
	}

	@Override
	public int hashCode() {
		return desc.getExprString().hashCode();
	}

	public HashSet<SDerivedColumn> getParams() {
		return directlyConnected;
	}

	@Override
	public void setup(int i) {
		for (ExprNodeDesc d : desc.getParameters()) {
			for (ExprNodeColumnDesc cd : SDerivedColumn.extractDirectColumnDescs(d)) {
				String n = cd.getColumn();
				String t = cd.getTabAlias();

				if (n == null) {
					n = "";
				}
				if (t == null) {
					t = "";
				}

				for (SOperator p : sop.parents) {
					for (SDerivedColumn c : p.columns) {
						if (c.equals(n, t)) {
							directlyConnected.add(c);
							return;
						}
					}
				}
			}
		}
	}

	@Override
	public HashSet<SBaseColumn> getBaseColumn() {
		return null;
	}
}