package org.apache.hadoop.hive.ql.cs;

import java.util.HashSet;

import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;

public class SConstant extends SDerivedColumn {
	
	ExprNodeConstantDesc desc;
	
	public SConstant(String name, String tableAlias, SOperator sop, ExprNodeConstantDesc desc) {
		super(name, tableAlias, sop);
		this.desc = desc;
	}

	@Override
	public boolean isGeneratedByAggregate() {
		return false;
	}
	
	@Override
	public int hashCode() {
		return desc.getExprString().hashCode();
	}

	@Override
	public void setup(int i) {
		// do nothing
	}
	
	@Override
	public HashSet<SBaseColumn> getBaseColumn() {
		return new HashSet<SBaseColumn>();
	}
}