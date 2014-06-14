package org.apache.hadoop.hive.ql.abm.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ConditionJoin extends GenericUDF {

  private BinaryObjectInspector inputOI;
  private KeyWrapper keys;
  private List<RangeList> ranges;
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("This function takes at least two arguments of type CondGroup");
    }

    inputOI = (BinaryObjectInspector) arguments[0];
    keys = new KeyWrapper();
    ranges = new ArrayList<RangeList>();
    return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    StringBuilder builder = new StringBuilder();
    builder.append("Cond_Join(");
    boolean first = true;
    for (String arg : arg0) {
      if (!first) {
        builder.append(", ");
      }
      first = false;
      builder.append(arg);
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    if (first) {
      for (DeferredObject o : arg) {
        Object condGroupObj = o.get();
        keyParser.parseInto(inputOI.getStructFieldData(condGroupObj, keyField), inputKeys);
        rangeParser.append(inputOI.getStructFieldData(condGroupObj, rangeField), inputRanges);
      }
      first = false;
    }

    inputKeys.clear();

    int cursor = 0;
    for (DeferredObject o : arg) {
      Object condGroupObj = o.get();
      keyParser.parseInto(inputOI.getStructFieldData(condGroupObj, keyField), inputKeys);
      cursor += rangeParser.overwrite(inputOI.getStructFieldData(condGroupObj, rangeField), inputRanges, cursor);
    }

    return ret;
  }

}