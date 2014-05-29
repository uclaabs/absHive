package org.apache.hadoop.hive.ql.abm.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public class SrvCompareConstant  extends GenericUDF {

  protected PrimitiveObjectInspector inputIDOI;
  protected PrimitiveObjectInspector inputValueOI;
  protected Object ret = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length > 3) {
      throw new UDFArgumentException("This function takes at most three arguments: Srv_ID, Constant Value");
    }

    inputIDOI = (PrimitiveObjectInspector) arguments[0];
    inputValueOI = (PrimitiveObjectInspector) arguments[1];

    if(!(inputIDOI instanceof IntObjectInspector)) {
      throw new UDFArgumentException("Srv_ID must be integer!");
    }

    ret = this.initRet();
    return CondGroup.condGroupInspector;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Srv Comparison";
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {

    int id = ((IntObjectInspector)inputIDOI).get(arg[0].get());
    double value = Double.parseDouble(inputValueOI.getPrimitiveJavaObject(arg[1].get()).toString());

    this.updateRet(id, value);
    return this.ret;
  }


  protected Object initRet()
  {
    CondGroup condGroup = new CondGroup();
    List<ConditionRange> rangeArray = new ArrayList<ConditionRange>(1);
    rangeArray.add(new ConditionRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    condGroup.addKey(-1);
    condGroup.addRangeList(rangeArray);
    return condGroup.toArray();
  }

  protected void updateRet(int id, double value)
  {
    // override it here
  }

}

