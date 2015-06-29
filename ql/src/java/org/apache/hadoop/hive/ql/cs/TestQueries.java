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

public class TestQueries {
		
	/**
	 * To run this project:
	 * Make sure you have hadoop 2.2.x. and hive 0.12 
	 * (Notice: This jar file only works with hive 0.12.)
	 * 
	 * Run steps.
	 * 0. Set env variables such as "Hadoop_Home" for hive to start up
	 * 1. Start hadoop, namenode, datanode, ... , etc
	 * 2. At root directory of hive 0.12, use command "ant clean package"
	 * 3. Go to directory by "cd ./build/dist/lib/"
	 * 4. replace the jar file "hive-exec-0.12.0.jar" with the file provided
	 * 5. Go to directory by "cd ../bin/"
	 * 6. "./hive"
	 * 
	 * After starting up the hive, modify the environment settings.
	 * Env settings: type in "Set hive.map.aggr=false;"
	 * 
	 * Use command "explain *sql_query*" to generate results.
	 * 
	 */
	//create table t (a int, b int);
	public static String[] data = new String[]{
		//all type 3
		"Set hive.map.aggr=false;",
		"explain select * from t;",
		"explain select sum(t.a) from t;",
		"explain select * from t x join t y on x.a = y.b;",
		"explain select * from t x join t y where x.a = y.b;",
		"explain select * from (select * from t) x join (select * from t) y;",
		"explain select * from (select count(1) a, sum(t.b) q from t where t.a = t.b group by b) r where r.a > 100;",
		"explain select t.a from t group by t.a;"
		//type 0
		//type 1
		//type 2
	};

}