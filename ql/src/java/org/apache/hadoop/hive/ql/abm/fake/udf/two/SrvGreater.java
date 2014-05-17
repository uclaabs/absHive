package org.apache.hadoop.hive.ql.abm.fake.udf.two;

import org.apache.hadoop.hive.ql.abm.fake.datatypes.SrvAno;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

public class SrvGreater extends GenericUDF {

//  private final StructObjectInspector structOI = null;
  private Object obj;
  private static final String opDisplayName = "Test Function Srv Greater";

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

	  if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes exactly two arguments.");
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
