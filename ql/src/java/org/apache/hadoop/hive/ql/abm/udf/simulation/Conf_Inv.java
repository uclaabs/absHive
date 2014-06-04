package org.apache.hadoop.hive.ql.abm.udf.simulation;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Conf_Inv extends GenericUDFWithSimulation {
  private static final String opDisplayName = "Conf_Inv_5_95";

  private final Double lowerPercent = 0.05;
  private final Double upperPercent = 0.95;

  public static List<String> columnName = Arrays.asList("Lower", "Upper");

  public static List<ObjectInspector> objectInspectorType = Arrays.asList(
      (ObjectInspector) PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      (ObjectInspector) PrimitiveObjectInspectorFactory.javaIntObjectInspector);

  public static StructObjectInspector oi = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentException("This function takes exactly 0 argument.");
    }

    return oi;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    int lower = (int) (samples.size() * lowerPercent);
    int upper = (int) (samples.size() * upperPercent);

    return new Object[]{lower, upper};
  }

	@Override
  public String getDisplayString(String[] children) {
	  return opDisplayName;
  }
}