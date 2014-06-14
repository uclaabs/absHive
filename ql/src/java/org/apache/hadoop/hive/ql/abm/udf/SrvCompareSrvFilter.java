package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvIO;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

public abstract class SrvCompareSrvFilter extends CompareUDF {

  protected BooleanWritable ret = new BooleanWritable(false);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes two arguments: Srv, Srv");
    }

    super.initialize(arguments);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    byte[] bytes1 = srvOI.getPrimitiveWritableObject(arg[0].get()).getBytes();
    double[] bound1 = SrvIO.getBound(bytes1);
    byte[] bytes2 = srvOI.getPrimitiveWritableObject(arg[1].get()).getBytes();
    double[] bound2 = SrvIO.getBound(bytes2);

    updateRet(bound1[0], bound1[1], bound2[0], bound2[1]);
    return ret;
  }

  protected abstract void updateRet(double lower1, double lower2, double upper1, double upper2);

}
