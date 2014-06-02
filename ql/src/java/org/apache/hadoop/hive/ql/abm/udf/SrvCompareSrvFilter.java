package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

public abstract class SrvCompareSrvFilter extends CompareUDF {

  protected BooleanWritable ret = new BooleanWritable(false);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 4) {
      throw new UDFArgumentException("This function takes four arguments: Srv[], Srv_ID[]");
    }

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    double lower1 = elemOI.get(srvOI.getListElement(arg[0], 0));
    double upper1 = elemOI.get(srvOI.getListElement(arg[0], 1));
    double lower2 = elemOI.get(srvOI.getListElement(arg[1], 0));
    double upper2 = elemOI.get(srvOI.getListElement(arg[1], 1));
    updateRet(lower1, lower2, upper1, upper2);
    return ret;
  }

  protected abstract void updateRet(double lower1, double lower2, double upper1, double upper2);

}
