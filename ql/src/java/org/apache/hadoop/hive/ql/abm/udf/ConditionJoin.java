package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ConditionJoin extends GenericUDF {

  private Object[] ret = new Object[2];
  private Object[] inputKeys = new Object[2];
  private Object[] inputRanges = new Object[2];
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException("This function takes two arguments: CondGroup, CondGroup");
    }
    
    Object[] keys = new Object[1];
    Object[] ranges = new Object[1];;
    ret[0] = keys;
    ret[1] = ranges;
    
    return CondGroup.condGroupInspector;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Function for Cond Group Join";
  }
  
  protected void parseCondGroupObj(Object condGroupObjs, int i)
  {
    Object[] condGroupObj = (Object[]) condGroupObjs;
    Object[] keyObjs = (Object[])condGroupObj[0];
    Object[] rangeObjs = (Object[])condGroupObj[1];
    
    inputKeys[i] = keyObjs[0];
    inputRanges[i] = rangeObjs[0];
  }
  
  protected int getLength(int i)
  {
    if(inputKeys[i] == null)
      return 0;
    else
      return ((Object[])inputKeys[i]).length;
  }

  @Override
  public Object evaluate(DeferredObject[] arg) throws HiveException {

    
   int size0, size1;
   
   parseCondGroupObj(arg[0].get(), 0);
   parseCondGroupObj(arg[1].get(), 1);
   
   size0 = getLength(0);
   size1 = getLength(1);
   
   // System.out.println(size0 + "\t" + size1);
   
   Object[] keys = new Object[size0 + size1];
   Object[] ranges = new Object[size0 + size1];
   
   Object[] keyObj0 = (Object[]) inputKeys[0];
   Object[] keyObj1 = (Object[]) inputKeys[1];
   Object[] rangeObj0 = (Object[]) inputRanges[0];
   Object[] rangeObj1 = (Object[]) inputRanges[1];
   
   for(int i = 0; i < size0; i ++)
   {
     keys[i] = keyObj0[i];
     ranges[i] = rangeObj0[i];
   }
   
   for(int i = 0; i < size1; i ++)
   {
     keys[i + size0] = keyObj1[i];
     ranges[i + size0] = rangeObj1[i];
   }
    
   ((Object[])ret[0])[0] = keys;
   ((Object[])ret[1])[0] = ranges;
   
   // System.out.println("Cond Join Finish");
   
    return this.ret;
  }


}

