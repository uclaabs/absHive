package org.apache.hadoop.hive.ql.abm.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapperParser;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeMatrixParser;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ConditionJoin extends GenericUDF {

  private Object[] ret = new Object[2];
  private KeyWrapper inputKeys = new KeyWrapper();
  private List<Object> inputRanges = new ArrayList<Object>();
  private KeyWrapperParser keyParser = null;
  private RangeMatrixParser rangeParser = null;
  private StructObjectInspector inputOI;
  private StructField keyField;
  private StructField rangeField;
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes two arguments: CondGroup, CondGroup");
    }
    inputOI = (StructObjectInspector) arguments[0];
    List<? extends StructField> fields = inputOI.getAllStructFieldRefs();
    keyField = fields.get(0);
    rangeField = fields.get(1);
    
    keyParser = new KeyWrapperParser(keyField.getFieldObjectInspector());
    rangeParser = new RangeMatrixParser(rangeField.getFieldObjectInspector());
    
    return arguments[0];
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Cond Group Join";
  }
  
  protected void parseCondGroupObj(Object condGroupObj, int i) {
    Object keyObj = inputOI.getStructFieldData(condGroupObj, keyField);
    Object rangeObj = inputOI.getStructFieldData(condGroupObj, rangeField);
    
    keyParser.parseInto(keyObj, inputKeys);
    rangeParser.shallowCopyInto(rangeObj, inputRanges);
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
   
   this.inputKeys.clear();
   this.inputRanges.clear();
   
   parseCondGroupObj(arg[0].get(), 0);
   parseCondGroupObj(arg[1].get(), 1);
   
   ret[0] = this.inputKeys;
   ret[1] = this.inputRanges;
   
   return this.ret;
  }


}

