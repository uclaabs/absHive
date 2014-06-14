package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

public class DiscreteSrvParser extends Parser {

  private final BinaryObjectInspector oi;

  public DiscreteSrvParser(ObjectInspector oi) {
    super(oi);
    this.oi = (BinaryObjectInspector) oi;
  }

  public DiscreteSrv parse(Object o) {
    byte[] buf = oi.getPrimitiveWritableObject(o).getBytes();

    try {
      BytesInput in = new BytesInput(buf);
      int len = in.readInt();
      DiscreteSrv srv = new DiscreteSrv(len - 2);
      in.readDouble();
      in.readDouble();
      for(int i = 2; i < len; i ++) {
        srv.add(in.readDouble());
      }
      return srv;
    } catch (IOException e) {

      return null;
    }
  }

}
