package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class DiscreteSrvParser extends SrvParser {

  public DiscreteSrvParser(ObjectInspector oi, int from, int to) {
    super(oi, from, to);
  }

  public double[] parse(Object o) {
    for(int i = 0; i < fields.length; ++i) {
      os[i] = oi.getStructFieldData(o, fields[i]);
    }

    int len = lois[0].getListLength(os[0]) - 2;
    double[] srvs = new double[len * fields.length];

    // read one double value from each list at a time
    for(int i = 2; i < len; ++i) {
     for(int j = 0; j < fields.length; ++j) {
       srvs[i - 2] = eois[j].get(lois[j].getListElement(os[j], i));
     }
    }

    return srvs;
  }

}
