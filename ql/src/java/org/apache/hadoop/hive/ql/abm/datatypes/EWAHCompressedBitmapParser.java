package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class EWAHCompressedBitmapParser {

  private final BinaryObjectInspector oi;

  public EWAHCompressedBitmapParser(ObjectInspector oi) {
    this.oi = (BinaryObjectInspector) oi;
  }

  public EWAHCompressedBitmap parse(Object o) {
    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
    BitmapObjectInputStream in = new BitmapObjectInputStream(oi.getPrimitiveWritableObject(o).getBytes());
    try {
      bitmap.readExternal(in);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return bitmap;
  }

}
