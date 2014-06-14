package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.util.List;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineageIO {

  public static BytesOutput out = new BytesOutput();

  public static byte[] serialize(List<EWAHCompressedBitmap> o) {
    int len = IOUtils.INT_SIZE;
    for (EWAHCompressedBitmap e : o) {
      len += IOUtils.estimateBitmap(e);
    }

    out.setBuffer(new byte[len]);
    try {
      out.writeInt(o.size());
      for (EWAHCompressedBitmap e : o) {
        IOUtils.serializeBitmap(e, out);
      }
    } catch (IOException e1) {
      return null;
    }
    return out.getBuffer();
  }

  public static EWAHCompressedBitmap[] deserialize(BytesInput in) {
    try {
      int len = in.readInt();
      EWAHCompressedBitmap[] ret = new EWAHCompressedBitmap[len];
      for (int i = 0; i < len; ++i) {
        ret[i] = IOUtils.deserialzieBitmap(in);
      }
      return ret;
    } catch (IOException e1) {
      return null;
    }
  }

}
