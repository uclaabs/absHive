package org.apache.hadoop.hive.ql.cs;

import java.util.HashSet;

public class SBaseColumn extends SAbstractColumn {

	/**
	 * construct from name and tableAlias
	 * @return
	 */
	public SBaseColumn(String name, String tableAlias) {
		super(name, tableAlias);
	}
	
	public String getName() {
		return name;
	}

	public String toString() {
		return "BASE: " + "(" + tableAlias +")"+ "[" + name + "]";
	}
	

	@Override
	public boolean isGeneratedByAggregate() {
		return false;
	}

	@Override
	public HashSet<SBaseColumn> getBaseColumn() {
		HashSet<SBaseColumn> ret = new HashSet<SBaseColumn>();
		ret.add(this);
		
		return ret;
	}

	@Override
	public boolean isCorrelatedWithAggregate() {
		return false;
	}
}