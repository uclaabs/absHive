package org.apache.hadoop.hive.ql.abm.fake.datatypes;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Dis {

	Double n;
	Double count;
	Double avg;
	Double var;

	public static List<String> columnName = Arrays.asList("N","COUNT","AVG","VAR");
	public static ObjectInspector[] objectInspector={PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
		PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
		PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
		PrimitiveObjectInspectorFactory.javaDoubleObjectInspector};
	public static List<ObjectInspector> objectInspectorType = Arrays.asList(objectInspector);


	public Dis()
	{
		n = count = avg = var = null;
	}

	public Dis(double dn, double dc, double da, double dv)
	{
		n = dn;
		count = dc;
		avg = da;
		var = dv;
	}


	public Object[] toArray()
	{
		Object[] dis = {n, count, avg, var};
		return dis;
	}

}
