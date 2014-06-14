package org.apache.hadoop.hive.ql.abm.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class LineageTest extends AbstractGenericUDAFResolver {
  
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE)
    {
        throw new UDFArgumentException("Tid must be primitive data types!");
    }
    return new TestLinEvaluator();

  }
  
  public class TestLinEvaluator extends GenericUDAFEvaluator {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      // TODO Auto-generated method stub
      return null;
    }
    
  }


}
