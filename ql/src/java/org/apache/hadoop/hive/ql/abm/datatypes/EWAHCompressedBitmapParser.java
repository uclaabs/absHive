package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class EWAHCompressedBitmapParser extends Parser {

  private final BinaryObjectInspector oi;

  public EWAHCompressedBitmapParser(ObjectInspector oi) {
    super(oi);
    this.oi = (BinaryObjectInspector) oi;
  }

  public EWAHCompressedBitmap parse(Object o) {
    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
    byte[] buf = oi.getPrimitiveJavaObject(o);
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);

    ObjectInputStream in = null;
    try {
      in = new ObjectInputStream(bytesIn);
      bitmap.readExternal(in);
      return bitmap;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return null;
  }

}
