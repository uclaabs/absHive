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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;

public class SFunction extends SDerivedColumn {

	ExprNodeGenericFuncDesc desc;

	boolean isGenByAggr = false;

	public SFunction(String name, String tableAlias, SOperator sop, ExprNodeGenericFuncDesc desc) {
		super(name, tableAlias, sop);
		this.desc = desc;
	}

	@Override
	public int hashCode() {
		return desc.getExprString().hashCode();
	}

	@Override
	public void setup(int i) {
		for (ExprNodeColumnDesc cd : SDerivedColumn.extractDirectColumnDescs(desc)) {
			String n = cd.getColumn();
			String t = cd.getTabAlias();

			if (n == null) {
				n = "";
			}
			if (t == null) {
				t = "";
			}

			boolean found = false;

			for (SOperator p : sop.parents) {
				for (SDerivedColumn c : p.columns) {
					if (c.equals(n, t)) {
						directlyConnected.add(c);
						found = true;
						break;
					}
				}
				if (found) {
					break;
				}
			}
		}

		GenericUDF genericUDF = desc.getGenericUDF();
		List<ExprNodeDesc> descChildren = desc.getChildExprs();
		if (genericUDF instanceof GenericUDFIf) {
			for (ExprNodeColumnDesc cd : SDerivedColumn.extractColumnDescs((descChildren.get(0)))) {
				String n = cd.getColumn();
				String t = cd.getTabAlias();

				if (n == null) {
					n = "";
				}
				if (t == null) {
					t = "";
				}

				boolean found = false;

				for (SOperator p : sop.parents) {
					for (SDerivedColumn c : p.columns) {
						if (c.equals(n, t)) {
							indirectlyConnected.add(c);
							found = true;
							break;
						}
					}
					if (found) {
						break;
					}
				}
			}
		} else if (genericUDF instanceof GenericUDFCase) {
			for (int j = 0; j < descChildren.size(); j+= 2) {
				for (ExprNodeColumnDesc cd : SDerivedColumn.extractColumnDescs((descChildren.get(j)))) {
					String n = cd.getColumn();
					String t = cd.getTabAlias();

					if (n == null) {
						n = "";
					}
					if (t == null) {
						t = "";
					}

					boolean found = false;

					for (SOperator p : sop.parents) {
						for (SDerivedColumn c : p.columns) {
							if (c.equals(n, t)) {
								indirectlyConnected.add(c);
								found = true;
								break;
							}
						}
						if (found) {
							break;
						}
					}
				}
			}
		}
	}

	@Override
	public HashSet<SBaseColumn> getBaseColumn() {
		for (SDerivedColumn c : directlyConnected) {
			HashSet<SBaseColumn> t = c.getBaseColumn();
			if (t == null || !t.isEmpty()) {
				return null;
			} else if (desc.getGenericUDF() instanceof GenericUDFIf) {
				return null;
			} else if (desc.getGenericUDF() instanceof GenericUDFCase) {
				return null;	
			}
		}

		return new HashSet<SBaseColumn>();
	}
}
