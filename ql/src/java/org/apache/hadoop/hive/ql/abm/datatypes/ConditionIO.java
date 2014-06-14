package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.util.List;

public class ConditionIO {

  private static final BytesOutput out = new BytesOutput();

  public static byte[] serialize(KeyWrapper key, List<RangeList> range) {
    int len = IOUtils.estimateIntArrayList(key);
    len += IOUtils.INT_SIZE;
    for (RangeList e : range) {
      len += IOUtils.estimateDoubleArrayList(e);
    }

    out.setBuffer(new byte[len]);
    try {
      IOUtils.serializeIntArrayList(key, out);
      out.writeInt(range.size());
      for (RangeList e : range) {
        IOUtils.serializeDoubleArrayList(e, out);
      }
    } catch (IOException e) {
      return null;
    }
    return out.getBuffer();
  }

}
