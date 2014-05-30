package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class CaseSumEvaluator extends SrvSumEvaluator {

  protected static class MyAggregationBuffer implements AggregationBuffer {

    Map<Integer, DoubleArrayList> groups = new LinkedHashMap<Integer, DoubleArrayList>();
    List<DoubleArrayList> partialResult = new ArrayList<DoubleArrayList>();
    List<Object> ret = new ArrayList<Object>();
    CaseSumComputation compute = new CaseSumComputation();
    double sum = 0;

    public void addBase(double value) {
      this.sum += value;
    }

    public void addBase(double partialSum, double partialSsum) {
      this.sum += partialSum;
    }

    public Object getPartialResult() {
      
      ret.clear();
      partialResult.clear();
      for (Map.Entry<Integer, DoubleArrayList> entry : groups.entrySet()) {
        partialResult.add(entry.getValue());
      }
      ret.add(this.sum);
      ret.add(0);
      ret.add(partialResult);
      
      return ret;
    }

    public void reset() {
      sum = 0;
      groups.clear();
      partialResult.clear();
      ret.clear();
      compute.clear();
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    CaseSumComputation compute = myagg.compute;
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
    compute.setTotalNumber(tot);
    compute.setBase(myagg.sum);
    for (Map.Entry<Integer, DoubleArrayList> entry : myagg.groups.entrySet()) {

      compute.setCurrentList(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i++;
    }
    return compute.getFinalResult();
  }

}
