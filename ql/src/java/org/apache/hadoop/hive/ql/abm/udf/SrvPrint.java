package org.apache.hadoop.hive.ql.abm.udf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvIO;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SrvPrint extends GenericUDF {

  private BinaryObjectInspector inputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    inputOI = (BinaryObjectInspector) arguments[0];
    return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    DoubleArrayList o = SrvIO.deserialize(inputOI.getPrimitiveWritableObject(arguments[0].get()).getBytes());
    return o;
  }

  @Override
  public String getDisplayString(String[] children) {
    // TODO Auto-generated method stub
    return null;
  }

}
