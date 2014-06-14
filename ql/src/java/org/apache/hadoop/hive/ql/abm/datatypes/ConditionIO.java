package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConditionIO {

  private static final BytesInput in = new BytesInput();
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

  public static BytesInput startParsing(byte[] buf) {
    in.setBuffer(buf);
    return in;
  }

  public static boolean checkBase(BytesInput in) {
    try {
      IOUtils.skipIntArray(in);
      int len = in.readInt();
      for (int i = 0; i < len; ++i) {
        if (!IOUtils.checkDoubleArray(in)) {
          in.rewind();
          return false;
        }
      }
      in.rewind();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  public static void parseKeyInto(BytesInput in, IntArrayList out) {
    try {
      IOUtils.deserializeIntArrayListInto(in, out);
    } catch (IOException e) {
    }
  }

  public static List<RangeList> parseRange(BytesInput in) {
    try {
      int len = in.readInt();
      List<RangeList> ret = new ArrayList<RangeList>();
      for (int i = 0; i < len; ++i) {
        RangeList out = new RangeList();
        IOUtils.deserializeDoubleArrayListInto(in, out);
        ret.add(out);
      }
      return ret;
    } catch (IOException e) {
      return null;
    }
  }

  public static void parseRangeInto(BytesInput in, List<RangeList> out) {
    try {
      int len = in.readInt();
      for (int i = 0; i < len; ++i) {
        IOUtils.deserializeDoubleArrayListInto(in, out.get(i));
      }
    } catch (IOException e) {
    }
  }

}
