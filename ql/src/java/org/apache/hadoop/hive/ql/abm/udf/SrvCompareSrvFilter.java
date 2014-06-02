package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SrvCompareSrvFilter extends GenericUDF {

  protected DoubleObjectInspector doubleOI;
  protected IntObjectInspector inputOI;
  protected ListObjectInspector inputSrvOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 4) {
      throw new UDFArgumentException("This function takes four arguments: Srv[], Srv_ID[]");
    }

    doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    inputOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    inputSrvOI = ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);

    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Srv Comparison";
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {

    // read the first two values which are the range of Srv
    double lower1 = doubleOI.get(inputSrvOI.getListElement(arg[0], 0));
    double upper1 = doubleOI.get(inputSrvOI.getListElement(arg[0], 1));

    double lower2 = doubleOI.get(inputSrvOI.getListElement(arg[1], 0));
    double upper2 = doubleOI.get(inputSrvOI.getListElement(arg[1], 1));

    return this.updateRet(lower1, lower2, upper1, upper2);
  }

  protected boolean updateRet(double lower1, double lower2, double upper1, double upper2)
  {
    // override it here
    return true;
  }

}
