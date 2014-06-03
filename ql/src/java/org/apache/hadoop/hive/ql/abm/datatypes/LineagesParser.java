package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineagesParser extends Parser {

  private final ListObjectInspector oi;
  private final EWAHCompressedBitmapParser parser;

  public LineagesParser(ObjectInspector oi) {
    super(oi);
    this.oi = (ListObjectInspector) oi;
    parser = new EWAHCompressedBitmapParser(this.oi.getListElementObjectInspector());
  }

  public EWAHCompressedBitmap[] parse(Object o) {
    int length = oi.getListLength(o);
    EWAHCompressedBitmap[] bitmaps = new EWAHCompressedBitmap[length];
    for (int i = 0; i < length; ++i) {
      bitmaps[i] = parser.parse(oi.getListElement(o, i));
    }
    return bitmaps;
  }

}
