package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class RangeMatrixParser extends Parser {
  
  private ListObjectInspector oi = null;
  private RangeListParser parser = null;

  public RangeMatrixParser(ObjectInspector oi) {
    super(oi);
    this.oi = (ListObjectInspector) oi;
    parser = new RangeListParser(this.oi.getListElementObjectInspector());
  }
  
  public List<RangeList> parse(Object o) {
    int length = oi.getListLength(o);
    List<RangeList> ret = new ArrayList<RangeList>(length);
    for (int i = 0; i < length; ++i) {
      ret.add(parser.parse(oi.getListElement(o, i)));
    }
    return ret;
  }
  
  public void parseInto(Object o, List<RangeList> ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      parser.parseInto(oi.getListElement(o, i), ret.get(i));
    }
  }
  
  public void shallowCopyInto(Object o, List<Object> ret) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      ret.add(oi.getListElement(o, i));
    }
  }
  
  public boolean isBase(Object o) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      if (!parser.isBase(oi.getListElement(o, i))) {
        return false;
      }
    }
    return true;
  }
  
}