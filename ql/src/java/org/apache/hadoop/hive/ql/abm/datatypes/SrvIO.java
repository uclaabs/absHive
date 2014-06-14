package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.io.IOException;

public class SrvIO {

  private static final BytesInput in = new BytesInput();
  private static final BytesOutput out = new BytesOutput();
  private static final double[] bounds = new double[2];

  public static byte[] serialize(DoubleArrayList o) {
    try {
      BytesOutput out = new BytesOutput(IOUtils.estimateDoubleArrayList(o));
      IOUtils.serializeDoubleArrayList(o, out);
      return out.getBuffer();
    } catch (IOException e) {
      return null;
    }
  }

  public static double[] getBound(byte[] buf) {
    out.setBuffer(buf);
    bounds[0] = in.readDouble();
    bounds[1] = in.readDouble();
    return bounds;
  }

}
