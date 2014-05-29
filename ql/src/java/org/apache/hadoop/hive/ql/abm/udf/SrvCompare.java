package org.apache.hadoop.hive.ql.abm.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SrvCompare  extends GenericUDF {
  
  protected DoubleObjectInspector doubleOI;
  protected PrimitiveObjectInspector inputIDOI;
  protected PrimitiveObjectInspector inputValueOI;
  protected ListObjectInspector inputSrvOI;
  protected Object ret = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 3) {
      throw new UDFArgumentException("This function takes three arguments: Srv_ID, Srv, Constant Value");
    }

    doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    inputValueOI = (PrimitiveObjectInspector) arguments[1];
    inputIDOI = (PrimitiveObjectInspector) arguments[2];
    inputSrvOI = ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);

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
    
    // read the first two values which are the range of Srv
    double lower = doubleOI.get(inputSrvOI.getListElement(arg[0], 0));
    double upper = doubleOI.get(inputSrvOI.getListElement(arg[0], 1));
    double value = Double.parseDouble(inputValueOI.getPrimitiveJavaObject(arg[1].get()).toString());
    int id = ((IntObjectInspector)inputIDOI).get(arg[2].get());
    
    this.updateRet(id, value, lower, upper);
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

  protected void updateRet(int id, double value, double lower, double upper)
  {
    // override it here
  }

}
