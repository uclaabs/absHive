package org.apache.hadoop.hive.ql.abm.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


public class CondMerge extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    // TODO: need a version of parameters.length == 0
    if (parameters.length <= 1)
    {
      if (parameters.length == 1){
        if (parameters[0].getCategory() != ObjectInspector.Category.STRUCT) {
          throw new UDFArgumentException("Condition_merge only process condition struct!; Current Type is: " + parameters[0].getCategory());
        }
      }
      return new CondMergeEvaluator();
    } else {
      //ignore now
      throw new UDFArgumentException("Condition_merge takes only at most one argument!");
    }
  }

}