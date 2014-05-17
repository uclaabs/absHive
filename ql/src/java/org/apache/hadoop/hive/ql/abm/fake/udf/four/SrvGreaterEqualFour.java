package org.apache.hadoop.hive.ql.abm.fake.udf.four;

import org.apache.hadoop.hive.ql.abm.fake.datatypes.SrvAno;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class SrvGreaterEqualFour extends GenericUDF {

  private final StructObjectInspector structOI = null;
  private Object obj;

  private static final String opDisplayName = "Test Function Srv GreaterEqual";

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 4) {
      throw new UDFArgumentException("This function takes exactly four arguments.");
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
	    assert (children.length == 4);
	    return opDisplayName + " " +  "(" + children[0] + ", " + children[1] + ", " + children[2] + ", " + children[3] + ")";
	  }
}
