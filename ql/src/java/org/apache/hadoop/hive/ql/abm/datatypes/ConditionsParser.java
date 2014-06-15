package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ConditionsParser {

  private final StructObjectInspector oi;
  private final StructField key;
  private final StructField ranges;
  private final KeyWrapperParser keyParser;
  private final RangeMatrixParser rangesParser;

  public ConditionsParser(ObjectInspector oi) {
    this.oi = (StructObjectInspector) oi;
    List<? extends StructField> fields = this.oi.getAllStructFieldRefs();
    key = fields.get(0);
    ranges = fields.get(1);
    keyParser = new KeyWrapperParser(key.getFieldObjectInspector());
    rangesParser = new RangeMatrixParser(ranges.getFieldObjectInspector());
  }

  public Conditions parse(Object o) {
    return new Conditions(keyParser.parse(oi.getStructFieldData(o, key)),
        rangesParser.parse(oi.getStructFieldData(o, ranges)));
  }

}
