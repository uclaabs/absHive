package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public class SrvCompareConstant  extends GenericUDF {

  protected PrimitiveObjectInspector inputIDOI;
  protected PrimitiveObjectInspector inputValueOI;
  protected StandardStructObjectInspector  structOI = null;
  protected Object ret = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes exactly two arguments: Srv_ID, Constant Value");
    }

    inputIDOI = (PrimitiveObjectInspector) arguments[0];
    inputValueOI = (PrimitiveObjectInspector) arguments[1];

    if(!(inputIDOI instanceof IntObjectInspector)) {
      throw new UDFArgumentException("Srv_ID must be integer!");
    }


    ret = this.initRet();
    structOI = ObjectInspectorFactory.getStandardStructObjectInspector(Condition.columnName, Condition.objectInspectorType);
    return structOI;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Srv Comparison";
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {

    int id = ((IntObjectInspector)inputIDOI).get(arg[0].get());
    double value = Double.parseDouble(inputValueOI.getPrimitiveJavaObject(arg[1].get()).toString());
    //double value = PrimitiveObjectInspectorUtils.getDouble(arg[1].get(), inputValueOI);
    // double value = (Double) inputValueOI.getPrimitiveJavaObject(arg[1].get());

    this.updateRet(id, value);
    return this.ret;
  }


  protected Object initRet()
  {
    Condition cond = new Condition(true);
    return cond.toArray();
  }

  protected void updateRet(int id, double value)
  {
    // override it here
  }

}

