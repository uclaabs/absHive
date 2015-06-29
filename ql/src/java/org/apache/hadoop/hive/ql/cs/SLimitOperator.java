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

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class SLimitOperator extends SOperator {

	int limit;

	public SLimitOperator(LimitOperator op, List<SOperator> parents, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		super(op, parents, ctx);
		limit = op.getConf().getLimit();
	}
	

	@Override
	public String prettyString() {
		return super.prettyString() + " Limit: " + limit;
	}
}