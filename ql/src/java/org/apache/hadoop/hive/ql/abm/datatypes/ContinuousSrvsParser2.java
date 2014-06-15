package org.apache.hadoop.hive.ql.abm.datatypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


public class ContinuousSrvsParser2 extends SrvParser {


  public ContinuousSrvsParser2(ObjectInspector oi, int from, int to) {
    super(oi, from, to);
  }

  public double[] parse(Object o) {
    int fieldLen = fields.length;
    for(int i = 0; i < fieldLen; i ++) {
      objs[i] = oi.getStructFieldData(o, fields[i]);
    }
    
    int srvLen = listOIs[0].getListLength(objs[0]) - 2;
    double[] srvs = new double[srvLen * fieldLen];
    // every time read two double values from each list and save it to srvs
    int index = 0;
    for(int i = 2; i < srvLen; i += 2) {
     for(int j = 0; j < fieldLen; j ++) {
       srvs[index] = valueOIs[j].get(listOIs[j].getListElement(objs[j], index));
       srvs[index + 1] = valueOIs[j].get(listOIs[j].getListElement(objs[j], index + 1));
       index += 2;
     }
    }
    return srvs;
  }

}