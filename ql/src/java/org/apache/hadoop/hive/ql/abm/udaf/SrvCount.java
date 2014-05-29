package org.apache.hadoop.hive.ql.abm.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class SrvCount extends AbstractGenericUDAFResolver {
  
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    if (parameters.length != 0) {
      throw new UDFArgumentException("srv_count takes 0 argument!");
    }

    return new SrvCountEvaluator();
  }

}
