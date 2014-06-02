package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;

public abstract class SrvCompareFilter extends CompareUDF {

  private PrimitiveObjectInspector valOI;
  protected BooleanWritable ret = new BooleanWritable(false);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentException("This function takes three arguments: Srv_ID, Srv, Constant Value");
    }

    super.initialize(arguments);

    valOI = (PrimitiveObjectInspector) arguments[1];
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    double lower = elemOI.get(srvOI.getListElement(arg[0].get(), 0));
    double upper = elemOI.get(srvOI.getListElement(arg[0].get(), 1));
    double value = PrimitiveObjectInspectorUtils.getDouble(arg[1].get(), valOI);
    updateRet(value, lower, upper);
    return ret;
  }

  protected abstract void updateRet(double value, double lower, double upper);

}
