package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class CaseCountEvaluator extends SrvCountEvaluator {

  protected static class MyAggregationBuffer implements AggregationBuffer {
    int baseCnt = 0;
    CaseCountComputation compute = new CaseCountComputation();

    public void addBase(int cnt) {
      baseCnt += cnt;
    }

    public void reset() {
      baseCnt = 0;
      compute.clear();
    }

  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    CaseCountComputation compute = myagg.compute;
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
