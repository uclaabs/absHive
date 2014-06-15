package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class SrvParser {

  protected StructObjectInspector oi;
  protected StructField[] fields;
  protected ListObjectInspector[] lois;
  protected DoubleObjectInspector[] eois;
  protected Object[] os;

  public SrvParser(ObjectInspector oi, int from, int to) {
    this.oi = (StructObjectInspector) oi;
    List<? extends StructField> allFields = this.oi.getAllStructFieldRefs();
    int len = to - from;
    fields = new StructField[len];
    lois = new ListObjectInspector[len];
    eois = new DoubleObjectInspector[len];
    os = new Object[len];

    for (int i = from, j = 0; i < to; ++i, ++j) {
      fields[j] = allFields.get(i);
      lois[j] = (ListObjectInspector) fields[j].getFieldObjectInspector();
      eois[j] = (DoubleObjectInspector) lois[j].getListElementObjectInspector();
    }
  }

}