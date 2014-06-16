package org.apache.hadoop.hive.ql.abm.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapperParser;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeMatrixParser;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ConditionJoin extends GenericUDF {

  private final KeyWrapper inputKeys = new KeyWrapper();
  private final List<RangeList> inputRanges = new ArrayList<RangeList>();
  private final Object[] ret = new Object[] {inputKeys, inputRanges};
  private boolean first = true;

  private List<KeyWrapperParser> keyParser = new ArrayList<KeyWrapperParser>();
  private List<RangeMatrixParser> rangeParser = new ArrayList<RangeMatrixParser>();
  private List<StructObjectInspector> inputOI = new ArrayList<StructObjectInspector>();
  private List<StructField> keyField = new ArrayList<StructField>();
  private List<StructField> rangeField = new ArrayList<StructField>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("This function takes at least two arguments of type CondGroup");
    }

    for(int i = 0; i < arguments.length; ++ i) {
      StructObjectInspector input = (StructObjectInspector) arguments[0];
      List<? extends StructField> fields = input.getAllStructFieldRefs();
      StructField key = fields.get(0);
      StructField range = fields.get(1);
      inputOI.add(input);
      keyField.add(key);
      rangeField.add(range);
      keyParser.add(new KeyWrapperParser(key.getFieldObjectInspector()));
      rangeParser.add(new RangeMatrixParser(range.getFieldObjectInspector()));
    }

    return CondList.condListOI;
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
      for(int i = 0; i < arg.length; ++ i) {
        Object condGroupObj = arg[i].get();
        keyParser.get(i).parseInto(inputOI.get(i).getStructFieldData(condGroupObj, keyField.get(i)), inputKeys);
        rangeParser.get(i).append(inputOI.get(i).getStructFieldData(condGroupObj, rangeField.get(i)), inputRanges);
      }
      first = false;
    }

    inputKeys.clear();

    int cursor = 0;
    for(int i = 0; i < arg.length; ++ i) {
      Object condGroupObj = arg[i].get();
      keyParser.get(i).parseInto(inputOI.get(i).getStructFieldData(condGroupObj, keyField.get(i)), inputKeys);
      cursor += rangeParser.get(i).overwrite(inputOI.get(i).getStructFieldData(condGroupObj, rangeField.get(i)), inputRanges, cursor);
    }

    return ret;
  }

}