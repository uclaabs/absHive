package org.apache.hadoop.hive.ql.abm.udf.simulation;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Mean extends GenericUDFWithSimulation {

  private final DoubleWritable ret = new DoubleWritable(0);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
//	  if (arguments.length != 0) {
//      throw new UDFArgumentException("This function takes exactly 0 argument.");
//    }

    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    double mean = 0.0;
    for (double sample: samples) {
      mean += sample;
    }
    mean = mean / samples.size();
    ret.set(mean);
    return ret;
  }

	@Override
  public String getDisplayString(String[] children) {
	  return "Mean";
  }
}