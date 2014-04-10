package org.apache.hadoop.hive.ql.abm.fake;

import org.apache.hadoop.hive.ql.abm.fake.datatypes.SrvAno;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

public class GenericEvaluator extends GenericUDAFEvaluator{

	private final Object obj = (new SrvAno()).toArray();

	@Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
			super.init(m, parameters);
      return ObjectInspectorFactory.getStandardStructObjectInspector(SrvAno.columnName, SrvAno.objectInspectorType);
}


	static class MkMapAggregationBuffer implements AggregationBuffer {
		double test;
		}

	@Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		MkMapAggregationBuffer ret = new MkMapAggregationBuffer();
		ret.test = 0.0;
	  return ret;
  }

	@Override
  public void iterate(AggregationBuffer arg0, Object[] arg1) throws HiveException {
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer)arg0;
		myagg.test += 1;
  }

	@Override
  public void merge(AggregationBuffer arg0, Object arg1) throws HiveException {
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer)arg0;
		myagg.test += 1;

  }

	@Override
  public void reset(AggregationBuffer arg0) throws HiveException {
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer)arg0;
		myagg.test = 0;

  }

	@Override
  public Object terminate(AggregationBuffer arg0) throws HiveException {
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer)arg0;
		myagg.test += 1;
	  return obj;
  }

	@Override
  public Object terminatePartial(AggregationBuffer arg0) throws HiveException {
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer)arg0;
		myagg.test += 1;
	  return obj;
  }

}
