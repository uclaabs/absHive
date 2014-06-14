package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvIO;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public abstract class SrvCompareSrv extends CompareUDF {

  private IntObjectInspector idOI;
  protected CondList ret = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 4) {
      throw new UDFArgumentException("This function takes four arguments: Srv, Srv, Srv_ID, Srv_ID");
    }

    super.initialize(arguments);

    idOI = (IntObjectInspector) arguments[2];

    ret = initRet();
    return CondList.condListOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    byte[] bytes1 = srvOI.getPrimitiveJavaObject(arg[0]);
    double[] bound1 = SrvIO.getBound(bytes1);
    byte[] bytes2 = srvOI.getPrimitiveJavaObject(arg[1]);
    double[] bound2 = SrvIO.getBound(bytes2);
    int id1 = (idOI).get(arg[2].get());
    int id2 = (idOI).get(arg[3].get());

    updateRet(id1, id2, bound1[0], bound1[1], bound2[0], bound2[1]);
    return ret.toArray();
  }

  protected CondList initRet() {
    CondList condList = new CondList();
    condList.addKey(-1);
    condList.addKey(-1);
    condList.addRangeValue(Double.NaN);
    return condList;
  }

  protected abstract void updateRet(int id1, int id2, double lower1, double lower2, double upper1, double upper2);

}
