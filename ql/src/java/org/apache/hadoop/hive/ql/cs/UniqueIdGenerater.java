package org.apache.hadoop.hive.ql.cs;

public class UniqueIdGenerater {
	private static Long count = 10L;
	
	public static Long getNextId() {
		count += 1L;
		return count;
	}
}