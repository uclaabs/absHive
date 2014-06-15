package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public class SrvParser extends Parser {
  
  protected StructObjectInspector oi;
  protected StructField[] fields;
  protected ListObjectInspector[] listOIs;
  protected DoubleObjectInspector[] valueOIs;
  protected Object[] objs;

  public SrvParser(ObjectInspector oi, int from, int to) {
    super(oi);
    this.oi = (StructObjectInspector) oi;
    List<? extends StructField> allFields = this.oi.getAllStructFieldRefs();
    fields = new StructField[to - from];
    listOIs = new ListObjectInspector[to - from];
    valueOIs = new DoubleObjectInspector[to - from];
    objs = new Object[to - from];
    
    for (int i = from, j = 0; i < to; ++i, ++j) {
      fields[j] = allFields.get(i);
      listOIs[j] = (ListObjectInspector) fields[j].getFieldObjectInspector();
      valueOIs[j] = (DoubleObjectInspector) listOIs[j].getListElementObjectInspector();
    }
  }

}