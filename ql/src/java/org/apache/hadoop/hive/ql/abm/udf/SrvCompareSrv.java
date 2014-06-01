package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SrvCompareSrv extends GenericUDF {
  
  protected DoubleObjectInspector doubleOI;
  protected LongObjectInspector inputOI;
  protected ListObjectInspector inputSrvOI;
  protected Object ret = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 4) {
      throw new UDFArgumentException("This function takes four arguments: Srv[], Srv_ID[]");
    }

    doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    inputOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    inputSrvOI = ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
   
    ret = this.initRet();
    return CondList.condListOI;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Srv Comparison";
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    
    // read the first two values which are the range of Srv
    double lower1 = doubleOI.get(inputSrvOI.getListElement(arg[0], 0));
    double upper1 = doubleOI.get(inputSrvOI.getListElement(arg[0], 1));
    
    double lower2 = doubleOI.get(inputSrvOI.getListElement(arg[1], 0));
    double upper2 = doubleOI.get(inputSrvOI.getListElement(arg[1], 1));
    
    long id1 = (inputOI).get(arg[2].get());
    long id2 = (inputOI).get(arg[3].get());
    
    this.updateRet(id1, id2, lower1, lower2, upper1, upper2);
    return this.ret;
  }


  protected Object initRet()
  {
    CondList condList = new CondList();
    condList.addKey(-1);
    condList.addKey(-1);
    condList.addPairRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    return condList.toArray();
  }

  protected void updateRet(long id1, long id2, double lower1, double lower2, double upper1, double upper2)
  {
    // override it here
  }

}
