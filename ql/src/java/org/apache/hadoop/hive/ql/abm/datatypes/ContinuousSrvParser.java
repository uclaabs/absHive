package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

public class ContinuousSrvParser extends Parser {

  private final BinaryObjectInspector oi;

  public ContinuousSrvParser(ObjectInspector oi) {
    super(oi);
    this.oi = (BinaryObjectInspector) oi;
  }

  public ContinuousSrv parse(Object o) {

    byte[] buf = oi.getPrimitiveWritableObject(o).getBytes();

    try {
      BytesInput in = new BytesInput(buf);
      int len = in.readInt();
      ContinuousSrv srv = new ContinuousSrv(len - 2);
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
