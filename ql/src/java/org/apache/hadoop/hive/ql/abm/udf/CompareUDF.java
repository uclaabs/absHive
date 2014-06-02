package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public abstract class CompareUDF extends GenericUDF {

  protected ListObjectInspector srvOI;
  protected DoubleObjectInspector elemOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    srvOI = (ListObjectInspector) arguments[0];
    elemOI = (DoubleObjectInspector) srvOI.getListElementObjectInspector();
    return null;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Srv Comparison";
  }

}
