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

  private KeyWrapperParser keyParser = null;
  private RangeMatrixParser rangeParser = null;
  private StructObjectInspector inputOI;
  private StructField keyField;
  private StructField rangeField;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("This function takes at least two arguments of type CondGroup");
    }

    inputOI = (StructObjectInspector) arguments[0];
    List<? extends StructField> fields = inputOI.getAllStructFieldRefs();
    keyField = fields.get(0);
    rangeField = fields.get(1);
    keyParser = new KeyWrapperParser(keyField.getFieldObjectInspector());
    rangeParser = new RangeMatrixParser(rangeField.getFieldObjectInspector());

    return CondList.condListOI;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Cond Join";
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