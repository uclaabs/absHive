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