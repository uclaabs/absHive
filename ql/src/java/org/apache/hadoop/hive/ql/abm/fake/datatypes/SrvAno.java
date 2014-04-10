package org.apache.hadoop.hive.ql.abm.fake.datatypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

public class SrvAno {

	List<Anno> annotations;
	List<Cond> conditions;

	public static List<String> columnName = Arrays.asList("ANNOTATIONS","CONDITIONS");

final static ObjectInspector[] objectInspector={
	ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(Anno.columnName,Anno.objectInspectorType)),
	ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(Cond.columnName,Cond.objectInspectorType))};
public final static List<ObjectInspector> objectInspectorType=Arrays.asList(objectInspector);


	public SrvAno()
	{
		annotations = new ArrayList<Anno>();
		conditions = new ArrayList<Cond>();
	}

	public SrvAno(double n, double count, double avg, double var, int t)
	{
		annotations = new ArrayList<Anno>();
		annotations.add(new Anno(n, count, avg, var, t));
		conditions = new ArrayList<Cond>();
	}

	public Object toArray()
	{
		Object[] srvano = {this.getAnoObject(), this.getDisObject()};
		return srvano;
	}

	private Object getAnoObject()
	{
		Object[] objs = new Object[annotations.size()];
		for(int i = 0; i < annotations.size(); i ++) {
      objs[i] = annotations.get(i).toArray();
    }
		return objs;
	}

	private Object getDisObject()
	{
		Object[] objs = new Object[conditions.size()];
		for(int i = 0; i < conditions.size(); i ++) {
      objs[i] = conditions.get(i).toArray();
    }
		return objs;
	}



}
