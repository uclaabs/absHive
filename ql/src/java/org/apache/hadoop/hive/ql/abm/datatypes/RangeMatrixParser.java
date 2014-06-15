package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class RangeMatrixParser {

  private ListObjectInspector oi = null;
  private RangeListParser parser = null;

  public RangeMatrixParser(ObjectInspector oi) {
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

  public void append(Object o, List<RangeList> ret) throws HiveException {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      ret.add(parser.parse(oi.getListElement(o, i)));
    }
  }

  public int overwrite(Object o, List<RangeList> ret, int start) throws HiveException {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      RangeList target = ret.get(i + start);
      target.clear();
      parser.parseInto(oi.getListElement(o, i), target);
    }
    return length;
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
