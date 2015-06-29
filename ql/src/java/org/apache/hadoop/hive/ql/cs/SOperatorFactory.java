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
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

@SuppressWarnings("unchecked")
public class SOperatorFactory {

	/**
	 * Generate the corresponding SOperator Tree given Operator Tree
	 * @param rootOp
	 */
	@SuppressWarnings("rawtypes")
	public static SOperator generateSOperatorTree(Operator rootOp,
			LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		if (rootOp == null) return null;
		//make sure there is only one FileSinkOperator
		//ignore
		while (rootOp instanceof FileSinkOperator) {
			rootOp = (Operator) rootOp.getParentOperators().get(0);
		}
		
		List<Operator> lst = rootOp.getParentOperators();
		List<SOperator> parents = new ArrayList<SOperator>();

		if (lst != null) {
			for (Operator l: lst) {
				parents.add(generateSOperatorTree(l, ctx));
			}
		}
		
		//create node
		if (rootOp instanceof FilterOperator) {
			return new SFilterOperator((FilterOperator) rootOp, parents, ctx);
		}
		else if (rootOp instanceof LimitOperator) {
			return new SLimitOperator((LimitOperator) rootOp, parents, ctx);
		}
		else if (rootOp instanceof GroupByOperator) {
			return new SGroupByOperator((GroupByOperator) rootOp, parents, ctx);
		}
		else if (rootOp instanceof ExtractOperator) {
			return new SExtractOperator((ExtractOperator) rootOp, parents, ctx);
		}
		else if (rootOp instanceof ReduceSinkOperator) {
			return new SReduceSinkOperator((ReduceSinkOperator) rootOp, parents, ctx);
		}
		else if (rootOp instanceof TableScanOperator) {
			return new STableScanOperator((TableScanOperator) rootOp, parents, ctx);
		}
		else if (rootOp instanceof SelectOperator) {
			return new SSelectOperator((SelectOperator) rootOp, parents, ctx);
		}
		else if (rootOp instanceof JoinOperator) {
			return new SJoinOperator((JoinOperator) rootOp, parents, ctx);
		}
		else {
			return new SOperator(rootOp, parents, ctx);
		}
	}
}