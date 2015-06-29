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