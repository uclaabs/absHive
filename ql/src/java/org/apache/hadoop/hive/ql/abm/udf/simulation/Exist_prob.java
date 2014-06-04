package org.apache.hadoop.hive.ql.abm.udf.simulation;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Exist_prob extends GenericUDFWithSimulation {
  private static final String opDisplayName = "Exist_prob";

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      //throw new UDFArgumentException("This function takes exactly two arguments.");
    }

    return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Double exist_prob = (samples.size() + 0.0) / numSimulation;

    return exist_prob;
  }

	@Override
  public String getDisplayString(String[] children) {
	  //assert (children.length == 2);
    //return opDisplayName + " " +  "(" + children[0] + ", " + children[1] + ")";
	  return opDisplayName;
  }
}