package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.io.IOException;

public class SrvIO {

  public static byte[] serialize(DoubleArrayList o) {
    try {
      BytesOutput out = new BytesOutput(IOUtils.estimateDoubleArrayList(o));
      IOUtils.serializeDoubleArrayList(o, out);
      return out.getBuffer();
    } catch (IOException e) {
      return null;
    }
  }

}
