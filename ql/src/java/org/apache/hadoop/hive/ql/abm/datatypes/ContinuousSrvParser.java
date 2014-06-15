package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ContinuousSrvParser extends SrvParser {

  public ContinuousSrvParser(ObjectInspector oi, int from, int to) {
    super(oi, from, to);
  }

  public double[] parse(Object o) {
    for(int i = 0; i < fields.length; ++i) {
      os[i] = oi.getStructFieldData(o, fields[i]);
    }

    int len = lois[0].getListLength(os[0]);
    double[] srvs = new double[(len - 2) * fields.length];

    // read two double values from each list at a time
    int pos = 0;
    for(int i = 2; i < len; i += 2) {
     for(int j = 0; j < fields.length; ++j) {
       srvs[pos++] = eois[j].get(lois[j].getListElement(os[j], i));
       srvs[pos++] = eois[j].get(lois[j].getListElement(os[j], i + 1));
     }
    }

    return srvs;
  }

}