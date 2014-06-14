package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionIO;
import org.apache.hadoop.hive.ql.abm.datatypes.Conditions;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

public class ConditionPrint extends GenericUDF {

  private BinaryObjectInspector inputOI;
  private Object[] ret = new Object[2];
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    
    inputOI = (BinaryObjectInspector) arguments[0];
    return CondList.condListOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    
    byte[] buf = inputOI.getPrimitiveWritableObject(arguments[0].get()).getBytes();
    Conditions conds = ConditionIO.deserialize(buf);
    ret[0] = conds.getKeys();
    ret[1] = conds.getRanges();
    
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }

}
