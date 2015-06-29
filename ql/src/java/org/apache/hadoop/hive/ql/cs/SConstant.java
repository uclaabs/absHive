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