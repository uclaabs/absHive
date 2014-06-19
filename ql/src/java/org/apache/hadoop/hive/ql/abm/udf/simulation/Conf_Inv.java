package org.apache.hadoop.hive.ql.abm.udf.simulation;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.abm.simulation.SimulationResult;
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

  private final DoubleArrayList buf = new DoubleArrayList();
  private final DoubleWritable[] ret = new DoubleWritable[] {new DoubleWritable(0), new DoubleWritable(0)};

  public static StructObjectInspector oi = ObjectInspectorFactory
      .getStandardStructObjectInspector(
          new ArrayList<String>(Arrays.asList("Lower", "Upper")),
          new ArrayList<ObjectInspector>(Arrays.asList(
              (ObjectInspector) PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
              (ObjectInspector) PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)));

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentException("This function takes exactly 0 argument.");
    }

    return oi;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    buf.clear();

    for(SimulationResult res : samples.samples) {
      for (double[][] smpls : res.samples) {
        if (smpls != null) {
          buf.add(smpls[smpls.length - 1][idx]);
        }
      }
    }

    DoubleArrays.quickSort(buf.elements(), 0, buf.size());

    ret[0].set(buf.getDouble((int) (buf.size() * lowerPercent)));
    ret[1].set(buf.getDouble((int) (buf.size() * upperPercent)));
    return ret;
  }

	@Override
  public String getDisplayString(String[] children) {
	  String ret = "Conf_Inv_5_95 (";
	  if (children != null) {
	    for (String child: children) {
	      ret += child + " ";
	    }
	  }
	  ret += ")";
	  return ret;
  }
}