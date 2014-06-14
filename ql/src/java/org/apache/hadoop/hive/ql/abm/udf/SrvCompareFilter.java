package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvIO;
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
    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes two arguments: Srv, Constant Value");
    }

    super.initialize(arguments);

    valOI = (PrimitiveObjectInspector) arguments[1];
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    byte[] bytes = srvOI.getPrimitiveJavaObject(arg[0]);
    double[] bound = SrvIO.getBound(bytes);
    double value = PrimitiveObjectInspectorUtils.getDouble(arg[1].get(), valOI);
    updateRet(value, bound[0], bound[1]);
    return ret;
  }

  protected abstract void updateRet(double value, double lower, double upper);

}
