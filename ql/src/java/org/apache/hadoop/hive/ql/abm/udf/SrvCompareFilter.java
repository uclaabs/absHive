package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SrvCompareFilter extends GenericUDF {
  
  protected DoubleObjectInspector doubleOI;
  protected PrimitiveObjectInspector inputValueOI;
  protected ListObjectInspector inputSrvOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 3) {
      throw new UDFArgumentException("This function takes three arguments: Srv_ID, Srv, Constant Value");
    }

    doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    inputValueOI = (PrimitiveObjectInspector) arguments[1];
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
    double lower = doubleOI.get(inputSrvOI.getListElement(arg[0], 0));
    double upper = doubleOI.get(inputSrvOI.getListElement(arg[0], 1));
    double value = Double.parseDouble(inputValueOI.getPrimitiveJavaObject(arg[1].get()).toString());
    return  this.updateRet(value, lower, upper);
  }

  protected boolean updateRet(double value, double lower, double upper)
  {
    // override it here
    return true;
  }

}
