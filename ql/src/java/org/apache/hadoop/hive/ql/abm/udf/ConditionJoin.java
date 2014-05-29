package org.apache.hadoop.hive.ql.abm.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ConditionJoin extends GenericUDF {

  private List<Object> ret = new ArrayList<Object>();
  private List<Integer> inputKeys = new ArrayList<Integer>();
  private List<Object> inputRanges = new ArrayList<Object>();
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes two arguments: CondGroup, CondGroup");
    }
    return CondGroup.condGroupInspector;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Cond Group Join";
  }
  
  protected void parseCondGroupObj(Object condGroupObj, int i)
  {
    ArrayList<Object> condGroupObjs = (ArrayList<Object>) condGroupObj;
    this.inputKeys.addAll((ArrayList<Integer>)condGroupObjs.get(0));
    this.inputRanges.addAll((ArrayList<Object>)condGroupObjs.get(1));
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {
   
   this.ret.clear();
   this.inputKeys.clear();
   this.inputRanges.clear();
   
   parseCondGroupObj(arg[0].get(), 0);
   parseCondGroupObj(arg[1].get(), 1);
   
   this.ret.add(this.inputKeys);
   this.ret.add(this.inputRanges);
   // System.out.println("Cond Join Finish");
   
    return this.ret;
  }


}

