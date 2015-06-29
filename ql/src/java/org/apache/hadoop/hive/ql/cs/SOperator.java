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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

class ColumnNotMappedException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	public ColumnNotMappedException(String s) {
		super(s);
	}
}

/**
 * Current Supported Operator Types:
 * 
 * 0. FileSinkOperator (Ignore)
 * 1. JoinOperator -- join FD
 * 2. SelectOperator
 * 3. FilterOperator -- filter FD
 * 4. ReduceSinkOperator
 * 5. TableScanOperator
 * 6. LimitOperator
 * 7. GroupByOperator
 * 8. ExtractOperator(order by)
 * 9. 
 * 
 * @author victor
 */
public class SOperator {

	protected Operator<? extends OperatorDesc> op;
	protected List<SOperator> parents;
	protected LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx;

	List<SDerivedColumn> columns = new ArrayList<SDerivedColumn>(); //output columns for this operator

	public SOperator(Operator<? extends OperatorDesc> op, List<SOperator> parents, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		this.op = op;
		this.parents = parents;
		this.ctx = ctx;
		buildColumns();
	}

	public void setup() {
		for (SOperator p : parents) {
			p.setup();
		}

		for (int i = 0; i < columns.size(); ++i) {
			columns.get(i).setup(i);
		}
	}

	protected void buildColumns() {
		RowSchema schema = op.getSchema();
		Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();
		//System.out.println(columnExprMap + " " + op.getClass());

		if (columnExprMap == null) {
			for (SDerivedColumn c: parents.get(0).columns) {
				columns.add(SDerivedColumn.create(
						c.name, c.tableAlias, this, 
						new ExprNodeColumnDesc(double.class, c.name, c.tableAlias, false)));
			}
		}
		else {
			for (ColumnInfo info: schema.getSignature()) {
				if (columnExprMap.get(info.getInternalName()) == null) {
					continue;
				}
				columns.add(SDerivedColumn.create(
						info.getInternalName(), info.getTabAlias(), this, 
						columnExprMap.get(info.getInternalName())));
			}
		}
	}

	public String toString() {
		return op.getClass() + "\n" + columns.toString();
	}

	public String prettyString() {
		return columns.toString();
	}

	public boolean hasNestedAggregates() {
		for (SOperator p : parents) {
			if(p.hasNestedAggregates()) {
				return true;
			}
		}

		return false;
	}
	
	public boolean hasCorrelatedAggregates() {
		for (SOperator p : parents) {
			if(p.hasCorrelatedAggregates()) {
				return true;
			}
		}

		return false;
	}

	public boolean hasUnionOperators() {
		for (SOperator p : parents) {
			if (p.hasUnionOperators()) {
				return true;
			}
		}

		if (op instanceof UnionOperator) {
			return true;
		}

		return false;
	}

	public boolean hasSpecialAggregates() {
		for (SOperator p : parents) {
			if (p.hasSpecialAggregates()) {
				return true;
			}
		}

		return false;
	}
	
	public boolean isComplex(boolean hasComplexOnTheWay) {
		for (SOperator p : parents) {
			if (p.isComplex(true)) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean hasAggregates() {
		for (SOperator p : parents) {
			if (p.hasAggregates()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean hasSelfJoin() {
		for (SOperator p : parents) {
			if (p.hasSelfJoin()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean hasDeduplication() {
		for (SOperator p : parents) {
			if (p.hasDeduplication()) {
				return true;
			}
		}
		
		return false;
	}
	
//	public boolean isEligible(HashSet<FD> rules, HashSet<SBaseColumn> bases) {
//		for (SOperator p : parents) {
//			HashSet<SBaseColumn> t = new HashSet<SBaseColumn>();
//			if (!p.isEligible(rules, t)) {
//				return false;
//			}
//			bases.addAll(t);
//		}
//		
//		return true;
//	}
}