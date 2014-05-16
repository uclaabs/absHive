package org.apache.hadoop.hive.ql.abm.fake.udf;

import org.apache.hadoop.hive.ql.abm.fake.datatypes.SrvAno;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


public class SrvPlus extends GenericUDF {

  private StructObjectInspector structOI = null;
  private Object obj;
  private static final String opDisplayName = "Test Function Srv Plus";

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes exactly two arguments.");
    }

    if (arguments[0].getCategory() == ObjectInspector.Category.STRUCT) {
      structOI = (StructObjectInspector) arguments[0];
    } else if(arguments[1].getCategory() == ObjectInspector.Category.STRUCT) {
      structOI = (StructObjectInspector) arguments[1];
    } else {
      throw new UDFArgumentException("One of parameters must be Srv Struct!");
    }

    obj = (new SrvAno()).toArray();

    return ObjectInspectorFactory.getStandardStructObjectInspector(SrvAno.columnName, SrvAno.objectInspectorType);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    return obj;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return opDisplayName + " " +  "(" + children[0] + ", " + children[1] + ")";
  }

}