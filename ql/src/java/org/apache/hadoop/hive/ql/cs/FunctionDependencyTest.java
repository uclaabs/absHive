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

import java.util.*;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;

class FD {
	Collection<SBaseColumn> determinists;
	Collection<SBaseColumn> dependents;
	
	public FD(Collection<SBaseColumn> determinists, Collection<SBaseColumn> dependents) {
		this.determinists = determinists;
		this.dependents = dependents;
	}
	
	public FD(SBaseColumn det, SBaseColumn dep) {
		if (det == null && dep == null) {
			System.out.println("Fatal Error: Det and Dep passed in are all NULL values");
		}
		
		Collection<SBaseColumn> detCollection = new HashSet<SBaseColumn>();
		Collection<SBaseColumn> depCollection = new HashSet<SBaseColumn>();
		if (det != null) {
			detCollection.add(det);
		}
		
		if (dep != null) {
			depCollection.add(dep);
		}
		
		this.determinists = detCollection;
		this.dependents = depCollection;
	}
	
	public static Collection<SBaseColumn> infer(Collection<SBaseColumn> det, Collection<FD> rules) {
		boolean flag = false;
		Collection<SBaseColumn> ret = new HashSet<SBaseColumn>(det);
		do
		{
			flag = false;
			for (FD rule : rules) {
				if (ret.containsAll(rule.determinists)) {
					ret.addAll(rule.dependents);
					flag = true;
				}
			}
		} while (flag);
		return ret;
	}
	
	public static boolean judge(Collection<SBaseColumn> inferred, Collection<SBaseColumn> originals) {
		return inferred.containsAll(originals);
	}
}

public class FunctionDependencyTest<T> {
	
	public FD doTests(SOperator sop) {

		return null;
		/*
		FD fd = new FD();
		
		//parents' FD first
		if (sop.parents.size() > 0) {
			for (SOperator parent: sop.parents) {
				fd.addAll(doTests(parent));
			}
		}

		Operator op = sop.op;
		if (op instanceof TableScanOperator) {
			fd.addAll(sop.rootFD);
		}
		
		//repeat adding dependencies to FD Collection until no longer changed 
		boolean changed = true;
		while (changed) {
			changed = false;
			
			
		}

		Collection<T> results = new HashCollection<T>();

		if (stat.dependency.conditions == null || conditions.containsAll(stat.dependency.conditions)) {
			results.addAll(stat.dependency.results);
			conditions.addAll(stat.dependency.results);
		}
		 */
	}
	
	public static void printInfo() {
		System.out.println("----------Calling FunctionDependencyTest-------------------");
	}

}