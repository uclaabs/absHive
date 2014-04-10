package org.apache.hadoop.hive.ql.abm.fake.datatypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Anno {

	List<Dis> distribution;
	Integer	type;
	List<Byte> lineage;

	public static List<String> columnName = Arrays.asList("DISTRIBUTIONS","TYPE", "LINEAGE");

	public static ObjectInspector[] objectInspector = {
		ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(Dis.columnName, Dis.objectInspectorType)),
		PrimitiveObjectInspectorFactory.javaIntObjectInspector, PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector};
	public static List<ObjectInspector> objectInspectorType = Arrays.asList(objectInspector);

	public Anno()
	{
		distribution = null;
		type = null;
		lineage = null;
	}

	public Anno(double n, double count, double avg, double var, int t)
	{
		distribution = new ArrayList<Dis>();
		distribution.add(new Dis(n, count, avg, var));
		type = t;
		lineage = null;
	}

	public void setLineage(Byte[] l)
	{
		lineage = Arrays.asList(l);
	}


	public Object toArray()
	{
		Object lineageObject = ObjectInspectorUtils.copyToStandardObject(this.lineage, PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
		Object[] anno = {this.getDisObject(), this.type, lineageObject};
		return anno;

	}

	private Object getDisObject()
	{
		Object[] diss = new Object[distribution.size()];
		for(int i = 0; i < distribution.size(); i ++) {
      diss[i] = distribution.get(i).toArray();
    }
		return diss;
	}





}
