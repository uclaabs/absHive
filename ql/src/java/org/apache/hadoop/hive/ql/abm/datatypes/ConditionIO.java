package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.util.ArrayList;
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

  public static Conditions deserialize(BytesInput in) {
    try {
      KeyWrapper key = new KeyWrapper();
      IOUtils.deserializeIntArrayListInto(in, key);
      int len = in.readInt();
      List<RangeList> range = new ArrayList<RangeList>();
      for (int i = 0; i < len; ++i) {
        RangeList r = new RangeList();
        IOUtils.deserializeDoubleArrayListInto(in, r);
        range.add(r);
      }
      return new Conditions(key, range);
    } catch (IOException e) {
      return null;
    }
  }

}
