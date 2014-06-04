package org.apache.hadoop.hive.ql.abm.udf.simulation;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Exist_prob extends GenericUDFWithSimulation {
  private static final String opDisplayName = "Exist_prob";

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentException("This function takes exactly 0 argument.");
    }

    return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    double exist_prob = (samples.size() + 0.0) / numSimulation;

    return exist_prob;
  }

	@Override
  public String getDisplayString(String[] children) {
	  return opDisplayName;
  }
}