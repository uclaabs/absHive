package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.util.List;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineageIO {

  public static byte[] serialize(List<EWAHCompressedBitmap> o) {
    int len = IOUtils.INT_SIZE;
    for (EWAHCompressedBitmap e : o) {
      len += IOUtils.estimateBitmap(e);
    }

    try {
      BytesOutput out = new BytesOutput(len);
      out.writeInt(o.size());

      for (EWAHCompressedBitmap e : o) {
        IOUtils.serializeBitmap(e, out);
      }
      return out.getBuffer();
    } catch (IOException e1) {
      return null;
    }
  }

}
