package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineagesParser extends Parser {

  private final BinaryObjectInspector oi;

  public LineagesParser(ObjectInspector oi) {
    super(oi);
    this.oi = (BinaryObjectInspector) oi;
  }

  public EWAHCompressedBitmap[] parse(Object o) {
    byte[] buf = oi.getPrimitiveWritableObject(o).getBytes();
    BytesInput in = new BytesInput(buf);
    return LineageIO.deserialize(in);
  }

}
