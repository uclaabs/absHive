package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
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
      throw new UDFArgumentException("This function takes four arguments: Srv[], Srv_ID[]");
    }

    super.initialize(arguments);

    idOI = (IntObjectInspector) arguments[2];

    ret = initRet();
    return CondList.condListOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
    // read the first two values which are the range of Srv
    double lower1 = elemOI.get(srvOI.getListElement(arg[0], 0));
    double upper1 = elemOI.get(srvOI.getListElement(arg[0], 1));

    double lower2 = elemOI.get(srvOI.getListElement(arg[1], 0));
    double upper2 = elemOI.get(srvOI.getListElement(arg[1], 1));

    int id1 = (idOI).get(arg[2].get());
    int id2 = (idOI).get(arg[3].get());

    updateRet(id1, id2, lower1, lower2, upper1, upper2);
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
