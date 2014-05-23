package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

public class ConditionJoin extends GenericUDF {


  private StandardListObjectInspector  outputOI = null;

  protected boolean flag = false;
  protected Object ret = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length > 2) {
      throw new UDFArgumentException("This function takes at most two arguments: ConditionList, Condition");
    }

    if (arguments.length == 1)
    {
      flag = true;
      Condition cond = new Condition(true);
      cond.setID(-1);
      cond.setRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
      ConditionList condList = new ConditionList(cond);
      this.ret = condList.toArray();
    }

    // inputStructOI = ObjectInspectorFactory.getStandardStructObjectInspector(Condition.columnName, Condition.objectInspectorType);

    outputOI = (StandardListObjectInspector) ConditionList.objectInspectorType;
    return outputOI;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Srv Comparison";
  }

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {

    if(flag)
    {
      Object condObj = arg0[0].get();
      ConditionList.update(this.ret, condObj);
    }
    else
    {
      Object condListObj = arg0[0].get();
      Object[] condObj = {arg0[1].get()};

      int matrixSize = outputOI.getListLength(condListObj);
      Object[] res = new Object[matrixSize + 1];
      for(int i = 0; i < matrixSize; i ++) {
        res[i] = outputOI.getListElement(condListObj, i);
      }
      res[matrixSize] = condObj;
      this.ret = res;
    }

    return this.ret;


  }


}

