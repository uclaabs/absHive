//package org.apache.hadoop.hive.ql.abm.datatypes;
//
//import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
//
//public class ContinuousSrvsParser extends SrvParser {
//
//  public ContinuousSrvsParser(ObjectInspector oi, int from, int to) {
//    super(oi, from, to);
//  }
//
//  public double[] parse(Object o) {
//    int fieldLen = fields.length;
//    for(int i = 0; i < fieldLen; i ++) {
//      objs[i] = oi.getStructFieldData(o, fields[i]);
//    }
//
//    int srvLen = listOIs[0].getListLength(objs[0]) - 2;
//    double[] srvs = new double[srvLen * fieldLen];
//    // every time read two double values from each list and save it to srvs
//    int index = 0;
//    for(int i = 2; i < srvLen; i += 2) {
//     for(int j = 0; j < fieldLen; j ++) {
//       srvs[index] = valueOIs[j].get(listOIs[j].getListElement(objs[j], index));
//       srvs[index + 1] = valueOIs[j].get(listOIs[j].getListElement(objs[j], index + 1));
//       index += 2;
//     }
//    }
//    return srvs;
//  }
//
//}

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
