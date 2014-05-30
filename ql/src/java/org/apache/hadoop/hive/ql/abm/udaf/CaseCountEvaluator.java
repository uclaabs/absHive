package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class CaseCountEvaluator extends SrvCountEvaluator {

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    // TODO: reuse
    CaseCountComputation compute = new CaseCountComputation();
    List<Merge> instructions = ins.getMergeInstruction();

    compute.setCount(this.tot);
    for(int i = 0; i < instructions.size(); i ++) {
      compute.addNewGroup();
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();

  }

}
