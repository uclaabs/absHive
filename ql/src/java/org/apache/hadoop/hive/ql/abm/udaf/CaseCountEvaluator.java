package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CaseCountEvaluator extends SrvCountEvaluator {

  private CaseCountComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    } else {
      compute = new CaseCountComputation();
      return doubleListOI;
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    compute.setCount(myagg.baseCnt);
    for(int i = 0; i < instructions.size(); i ++) {
      compute.addNewGroup();
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();

  }

}
