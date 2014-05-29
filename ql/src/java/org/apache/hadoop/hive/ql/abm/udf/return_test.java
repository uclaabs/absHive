package org.apache.hadoop.hive.ql.abm.udf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class return_test extends GenericUDF {
  
  private final List<DoubleArrayList> test = new ArrayList<DoubleArrayList>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    
    return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
    // return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    
    DoubleArrayList list = new DoubleArrayList();
    test.add(list);
    list.add(0);
    
//    Object[] ret = new Object[test.size()];
//    
//    for(int i = 0; i < test.size(); i ++)
//      ret[i] = test.get(i);
    
    return test;
  }

  @Override
  public String getDisplayString(String[] children) {
    // TODO Auto-generated method stub
    return "";
  };
  
  

}
