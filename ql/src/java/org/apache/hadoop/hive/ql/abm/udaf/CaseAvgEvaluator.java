package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class CaseAvgEvaluator extends SrvAvgEvaluator {
  
  protected static class MyAggregationBuffer implements AggregationBuffer {
    
    Map<Integer, DoubleArrayList> groups = new LinkedHashMap<Integer, DoubleArrayList>();
    List<DoubleArrayList> partialResult = new ArrayList<DoubleArrayList>();
    double baseSum = 0;
    int baseCnt = 0;
    
    public void addBase(double value) {
      this.baseSum += value;
      this.baseCnt += 1;
    }
    
    public void addBase(double partialSum, double partialSsum, int partialCnt) {
      baseSum += partialSum;
      baseCnt += partialCnt;
    }
    
    public Object getPartialResult() {
      Object[] ret = new Object[2];
      partialResult.clear();
      for(Map.Entry<Integer, DoubleArrayList> entry:groups.entrySet()) {
        partialResult.add(entry.getValue());
      }
      ret[0] = baseSum;
      ret[1] = 0;
      ret[2] = baseCnt;
      ret[3] = partialResult;
      return ret;
    }
    
    public void reset() {
      baseSum = 0;
      baseCnt = 0;
      groups.clear();
      partialResult.clear();
    }
  }
  
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    CaseAvgComputation compute = new CaseAvgComputation();
    List<Merge> instructions = ins.getMergeInstruction();
    
    int i = 0;
    compute.setTotalNumber(tot);
    compute.setBase(myagg.baseSum, myagg.baseCnt);
    for(Map.Entry<Integer, DoubleArrayList> entry: myagg.groups.entrySet()) {
      
      compute.setCurrentList(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i ++;
    }
    return compute.getFinalResult();
    
  }
  
  

}
