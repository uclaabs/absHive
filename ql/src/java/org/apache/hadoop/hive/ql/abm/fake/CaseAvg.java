package org.apache.hadoop.hive.ql.abm.fake;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


public class CaseAvg  extends AbstractGenericUDAFResolver {

	@Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
    return new GenericEvaluator();
	}

}
