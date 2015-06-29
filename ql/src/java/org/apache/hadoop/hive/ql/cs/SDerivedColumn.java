/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.cs;

import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;

public abstract class SDerivedColumn extends SAbstractColumn {

	protected SOperator sop;

	protected HashSet<SDerivedColumn> directlyConnected = new HashSet<SDerivedColumn>();

	protected HashSet<SDerivedColumn> indirectlyConnected = new HashSet<SDerivedColumn>();

	public SDerivedColumn(String name, String tableAlias, SOperator sop) {
		super(name, tableAlias);
		this.sop = sop;
	}

	@Override
	public boolean isGeneratedByAggregate() {
		for (SAbstractColumn c : directlyConnected) {
			if (c.isGeneratedByAggregate()) {
				return true;
			}
		}

		return false;
	}
	
	public abstract void setup(int i);

	@Override
	public String toString() {
		return super.toString() + " IDC: " + indirectlyConnected;
	}

	public static SDerivedColumn create(String name, String tableAlias, SOperator sop, ExprNodeDesc nodeDesc) {
		if (nodeDesc instanceof ExprNodeConstantDesc) {
			return new SConstant(name, tableAlias, sop, (ExprNodeConstantDesc) nodeDesc);
		} else if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
			return new SFunction(name, tableAlias, sop, (ExprNodeGenericFuncDesc) nodeDesc);
		} else if (nodeDesc instanceof ExprNodeColumnDesc) {
			return new SColumn(name, tableAlias, sop, (ExprNodeColumnDesc) nodeDesc);
		} else {
			System.out.println("Fatal Error: Unsuppored ExprNode Type!" + nodeDesc.getClass());
			return null;
		}
	}

	public static HashSet<ExprNodeColumnDesc> extractDirectColumnDescs(ExprNodeDesc nodeDesc) {
		HashSet<ExprNodeColumnDesc> ret = new HashSet<ExprNodeColumnDesc>();
		if (nodeDesc instanceof ExprNodeConstantDesc) {
			// do nothing
		} else if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
			ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) nodeDesc;
			GenericUDF genericUDF = fd.getGenericUDF();
			List<ExprNodeDesc> descChildren = fd.getChildExprs();
			if (genericUDF instanceof GenericUDFIf) {
				ret.addAll(extractDirectColumnDescs(descChildren.get(1)));
				ret.addAll(extractDirectColumnDescs(descChildren.get(2)));
			} else if (genericUDF instanceof GenericUDFCase) {
				for (int i = 1; i < descChildren.size(); i+= 2) {
					ret.addAll(extractDirectColumnDescs(descChildren.get(i)));
				}	
			} else {
				for (int i = 0; i < descChildren.size(); i++) {
					ret.addAll(extractDirectColumnDescs(descChildren.get(i)));
				}
			}
		} else if (nodeDesc instanceof ExprNodeColumnDesc) {
			ret.add((ExprNodeColumnDesc) nodeDesc);
		} else {
			System.out.println("Fatal Error: Unsuppored ExprNode Type!" + nodeDesc.getClass());
			return null;
		}
		return ret;

	}

	public static HashSet<ExprNodeColumnDesc> extractColumnDescs(ExprNodeDesc nodeDesc) {
		HashSet<ExprNodeColumnDesc> ret = new HashSet<ExprNodeColumnDesc>();
		if (nodeDesc instanceof ExprNodeConstantDesc) {
			// do nothing
		} else if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
			ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) nodeDesc;
			List<ExprNodeDesc> descChildren = fd.getChildExprs();
			for (int i = 0; i < descChildren.size(); i++) {
				ret.addAll(extractColumnDescs(descChildren.get(i)));
			}
		} else if (nodeDesc instanceof ExprNodeColumnDesc) {
			ret.add((ExprNodeColumnDesc) nodeDesc);
		} else {
			System.out.println("Fatal Error: Unsuppored ExprNode Type!" + nodeDesc.getClass());
			return null;
		}
		return ret;
	}
	
	@Override
	public boolean isCorrelatedWithAggregate() {
		for (SDerivedColumn c : directlyConnected) {
			if (c.isCorrelatedWithAggregate()) {
				return true;
			}
		}
		for (SDerivedColumn c : indirectlyConnected) {
			if (c.isCorrelatedWithAggregate()) {
				return true;
			}
		}
		
		return false;
	}
}