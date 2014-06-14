package org.apache.hadoop.hive.ql.abm.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.BytesInput;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionIO;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

public class ConditionJoin extends GenericUDF {

  private BinaryObjectInspector inputOI;
  private KeyWrapper keys;
  private List<RangeList> ranges;
  private boolean first;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("This function takes at least two arguments of type CondGroup");
    }

    inputOI = (BinaryObjectInspector) arguments[0];
    first = true;
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
        BytesInput in = ConditionIO.startParsing(inputOI.getPrimitiveWritableObject(condGroupObj).getBytes());
        keys = new KeyWrapper();
        ConditionIO.parseKeyInto(in, keys);
        ranges = ConditionIO.parseRange(in);
      }
      first = false;
      return new BytesWritable(ConditionIO.serialize(keys, ranges));
    }

    keys.clear();
    for (DeferredObject o : arg) {
      Object condGroupObj = o.get();
      BytesInput in = ConditionIO.startParsing(inputOI.getPrimitiveWritableObject(condGroupObj).getBytes());
      ConditionIO.parseKeyInto(in, keys);
      ConditionIO.parseRangeInto(in, ranges);
    }

    return new BytesWritable(ConditionIO.serialize(keys, ranges));
  }

}