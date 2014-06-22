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

public class Quantile extends GenericUDFWithSimulation {

  private static double quantilePct = 0.5;

  private final DoubleArrayList buf = new DoubleArrayList();
  private final DoubleWritable[] ret = new DoubleWritable[] {new DoubleWritable(0),
      new DoubleWritable(0), new DoubleWritable(0)};

  public static StructObjectInspector oi = ObjectInspectorFactory
      .getStandardStructObjectInspector(
          new ArrayList<String>(Arrays.asList("Quantile", "Mean", "Variance")),
          new ArrayList<ObjectInspector>(Arrays.asList(
              (ObjectInspector) PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
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

    double sum = 0;
    double ssum = 0;

    for (SimulationResult res : samples.samples) {
      for (double[][] smpls : res.samples) {
        if (smpls != null) {
          double v = smpls[smpls.length - 1][columnIndex];
          sum += v;
          ssum += v * v;
          buf.add(v);
        }
      }
    }

    DoubleArrays.quickSort(buf.elements(), 0, buf.size());


    if (buf.size() == 0) {
      ret[0].set(Double.NaN);
      ret[1].set(Double.NaN);
      ret[2].set(Double.NaN);
    } else {
      ret[0].set(buf.getDouble((int) (buf.size() * quantilePct)));
      ret[1].set(sum/buf.size());
      ret[2].set((ssum - (sum * sum)/buf.size()) / (buf.size()));
    }
    return ret;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    StringBuilder builder = new StringBuilder();
    builder.append("quantile(");
    boolean first = true;
    for (String arg : arg0) {
      if (!first) {
        builder.append(", ");
      }
      first = false;
      builder.append(arg);
    }
    builder.append(")");
    return builder.toString();
  }

  public static void setQuantile(int quant) {
    quantilePct = quant / 100.0;
  }

}