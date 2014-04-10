package org.apache.hadoop.hive.ql.abm.fake.datatypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Cond {

	List<Anno> annos;
	List<Double> lower;
	List<Double> upper;
	List<Integer> ctype;

	/*
	 * ctype-0: ( )
	 * ctype-1: ( ]
	 * ctype-2: [ )
	 * ctype-3: [ ]
	 */

	public static List<String> columnName = Arrays.asList("ANNOTATIONS","LOWER","UPPER","CTYPE");

	final static ObjectInspector[] objectInspector={
		ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(Anno.columnName, Anno.objectInspectorType)),
		ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
		ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
		ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector)};
	final static List<ObjectInspector> objectInspectorType=Arrays.asList(objectInspector);


	public Cond() {
		annos = null;
		lower = null;
		upper = null;
		ctype = null;
	}

	/*
	 * op-1 >
	 * op-2 >=
	 * op-3 <
	 * op-4 <=
	 */
	public Cond(Anno a, int op, double bound){

		annos = new ArrayList<Anno>();
		lower = new ArrayList<Double>();
		upper = new ArrayList<Double>();

		annos.add(a);

		if(op >= 1 && op <= 2)
		{
			lower.add(bound);
			upper.add(null);
		}
		else
		{
			lower.add(null);
			upper.add(bound);
		}

		if(op == 1||op == 3) {
      ctype.add(0);
    } else if(op == 4) {
      ctype.add(1);
    } else {
      ctype.add(2);
    }
	}

	public Object toArray()
	{
		Object[] cond = {this.getAnnoObject(), this.lower.toArray(), this.upper.toArray(), this.ctype.toArray()};
		return cond;
	}

	private Object getAnnoObject()
	{
		Object[] anoos = new Object[annos.size()];
		for(int i = 0; i < annos.size(); i ++) {
      anoos[i] = annos.get(i).toArray();
    }
		return anoos;
	}





}
