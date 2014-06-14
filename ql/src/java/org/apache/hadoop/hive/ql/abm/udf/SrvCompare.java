package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvIO;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public abstract class SrvCompare extends CompareUDF {

  private PrimitiveObjectInspector valOI;
  private IntObjectInspector idOI;
  protected CondList ret = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentException(
          "This function takes three arguments: Srv, Constant Value, Srv_ID");
    }

    super.initialize(arguments);

    valOI = (PrimitiveObjectInspector) arguments[1];
    idOI = (IntObjectInspector) arguments[2];

    ret = initRet();
    return CondList.condListOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    byte[] bytes = srvOI.getPrimitiveJavaObject(arg[0]);
    double[] bound = SrvIO.getBound(bytes);
    double value = PrimitiveObjectInspectorUtils.getDouble(arg[1].get(), valOI);
    int id = idOI.get(arg[2].get());

    updateRet(id, value, bound[0], bound[1]);
    return ret.toArray();
  }

  protected CondList initRet() {
    CondList condList = new CondList();
    condList.addKey(-1);
    condList.addRangeValue(Double.NaN);
    return condList;
  }

  protected abstract void updateRet(int id, double value, double lower, double upper);

}
