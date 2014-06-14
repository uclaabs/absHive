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
    if (parameters.length == 0) {
      return new DummyCondMergeEvaluator();
    } else if (parameters.length == 1) {
      if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentException(
            "cond_merge takes binary condition as input, but "
                + parameters[0].getCategory() + " is passed");
      }
      return new CondMergeEvaluator();
    } else {
      throw new UDFArgumentException("cond_merge takes at most one argument!");
    }
  }

}
