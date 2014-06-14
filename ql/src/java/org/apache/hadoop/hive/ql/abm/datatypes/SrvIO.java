package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.io.IOException;

public class SrvIO {

  private static final BytesInput in = new BytesInput();
  private static final BytesOutput out = new BytesOutput();
  private static final double[] bounds = new double[2];

  public static byte[] serialize(DoubleArrayList o) {
    out.setBuffer(new byte[IOUtils.estimateDoubleArrayList(o)]);
    try {
      IOUtils.serializeDoubleArrayList(o, out);
    } catch (IOException e) {
      return null;
    }
    return out.getBuffer();
  }

  public static double[] getBound(byte[] buf) {
    in.setBuffer(buf);
    try {
      in.readInt();
      bounds[0] = in.readDouble();
      bounds[1] = in.readDouble();
    } catch (IOException e) {
      return null;
    }
    return bounds;
  }

}
