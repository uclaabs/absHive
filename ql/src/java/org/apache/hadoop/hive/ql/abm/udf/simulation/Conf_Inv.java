package org.apache.hadoop.hive.ql.abm.udf.simulation;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Conf_Inv extends GenericUDFWithSimulation {

  private static double lowerPercent = 0.05;
  private static double upperPercent = 0.95;

  private final DoubleWritable[] ret = new DoubleWritable[] {new DoubleWritable(0), new DoubleWritable(0)};

  public static StructObjectInspector oi = ObjectInspectorFactory
      .getStandardStructObjectInspector(Arrays.asList("Lower", "Upper"), Arrays.asList(
          (ObjectInspector) PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
          (ObjectInspector) PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
//    if (arguments.length != 0) {
//      throw new UDFArgumentException("This function takes exactly 0 argument.");
//    }

    return oi;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    ret[0].set(samples.getDouble((int) (samples.size() * lowerPercent)));
    ret[1].set(samples.getDouble((int) (samples.size() * upperPercent)));
    return ret;
  }

	@Override
  public String getDisplayString(String[] children) {
	  return "Conf_Inv_5_95";
  }
}