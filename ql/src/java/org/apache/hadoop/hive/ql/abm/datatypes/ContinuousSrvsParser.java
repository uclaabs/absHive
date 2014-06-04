package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ContinuousSrvsParser extends Parser {

  private final StructObjectInspector oi;
  private final StructField[] fields;
  private final ContinuousSrvParser[] parsers;

  public ContinuousSrvsParser(ObjectInspector oi, int from, int to) {
    super(oi);
    this.oi = (StructObjectInspector) oi;
    List<? extends StructField> allFields = this.oi.getAllStructFieldRefs();
    fields = new StructField[to - from];
    parsers = new ContinuousSrvParser[to - from];
    for (int i = from, j = 0; i < to; ++i, ++j) {
      fields[j] = allFields.get(i);
      parsers[j] = new ContinuousSrvParser(fields[j].getFieldObjectInspector());
    }
  }

  public ContinuousSrv[] parse(Object o) {
    int length = parsers.length;
    ContinuousSrv[] ret = new ContinuousSrv[parsers.length];
    for (int i = 0; i < length; ++i) {
      ret[i] = parsers[i].parse(oi.getStructFieldData(o, fields[i]));
    }
    return ret;
  }

}
