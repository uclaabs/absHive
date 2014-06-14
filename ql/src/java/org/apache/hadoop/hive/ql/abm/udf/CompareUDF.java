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

  protected abstract String udfFuncName();

  @Override
  public String getDisplayString(String[] arg0) {
    StringBuilder builder = new StringBuilder();
    builder.append(udfFuncName());
    builder.append("(");
    boolean first = true;
    for (String arg : arg0) {
      if (!first) {
        builder.append(", ");
      }
      first = false;
      builder.append(arg);
    }
    builder.append(")");
    return builder.toString();
  }


}
