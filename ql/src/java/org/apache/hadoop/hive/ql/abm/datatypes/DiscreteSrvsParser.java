//package org.apache.hadoop.hive.ql.abm.datatypes;
//
//import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
//
//
//public class DiscreteSrvsParser extends SrvParser {
//
//  public DiscreteSrvsParser(ObjectInspector oi, int from, int to) {
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
//    // every time read one double value from each list and save it to srvs
//    int index = 0;
//    for(int i = 2; i < srvLen; i ++) {
//     for(int j = 0; j < fieldLen; j ++) {
//       srvs[index] = valueOIs[j].get(listOIs[j].getListElement(objs[j], index));
//       index ++;
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

public class DiscreteSrvsParser extends Parser {

  private final StructObjectInspector oi;
  private final StructField[] fields;
  private final DiscreteSrvParser[] parsers;

  public DiscreteSrvsParser(ObjectInspector oi, int from, int to) {
    super(oi);
    this.oi = (StructObjectInspector) oi;
    List<? extends StructField> allFields = this.oi.getAllStructFieldRefs();
    fields = new StructField[to - from];
    parsers = new DiscreteSrvParser[to - from];
    for (int i = from, j = 0; i < to; ++i, ++j) {
      fields[j] = allFields.get(i);
      parsers[j] = new DiscreteSrvParser(fields[j].getFieldObjectInspector());
    }
  }

  public DiscreteSrv[] parse(Object o) {
    int length = parsers.length;
    DiscreteSrv[] ret = new DiscreteSrv[parsers.length];
    for (int i = 0; i < length; ++i) {
      ret[i] = parsers[i].parse(oi.getStructFieldData(o, fields[i]));
    }
    return ret;
  }

}
