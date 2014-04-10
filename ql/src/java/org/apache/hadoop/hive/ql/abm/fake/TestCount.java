package org.apache.hadoop.hive.ql.abm.fake;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


public class TestCount  extends AbstractGenericUDAFResolver {


	@Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {

    if (parameters.length > 3) {
      throw new UDFArgumentException("count_srv takes at most three arguments!");
    }

    return new GenericEvaluator();
	}

}
