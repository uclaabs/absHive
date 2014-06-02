package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.ValueListParser;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

public abstract class SrvAggregationBuffer implements AggregationBuffer{
  
  protected Map<Integer, DoubleArrayList> groups = new LinkedHashMap<Integer, DoubleArrayList>();
  protected List<DoubleArrayList> partialResult = new ArrayList<DoubleArrayList>();
  protected List<Object> ret = new ArrayList<Object>();
  protected ValueListParser parser = null;
  
  public SrvAggregationBuffer(ValueListParser inputParser) {
    parser = inputParser;
  }

  public void addValue(int ins, double value) {
    if(ins >= 0) {
      DoubleArrayList lineageList = groups.get(ins);
      if (lineageList == null) {
        lineageList = new DoubleArrayList();
        lineageList.add(value);
        groups.put(ins, lineageList);
      } else {
        lineageList.add(value);
      }
    } else {
      processBase(value);
    }
  }
  
  public void addValueList(int ins, Object listObj) {
    DoubleArrayList lineageList = groups.get(ins);
    
    if(lineageList == null) {
      lineageList = parser.parse(listObj);
      groups.put(ins, lineageList);
    } else {
      parser.parseInto(listObj, lineageList);
    }
  }
  
  public abstract void processBase(double value);
  
  public void addGroupToRet() {
    ret.clear();
    partialResult.clear();
//    for (Map.Entry<Integer, DoubleArrayList> entry : groups.entrySet()) {
//      partialResult.add(entry.getValue());
//    }
    partialResult.addAll(groups.values());
    ret.add(partialResult);
  }
  
  public abstract Object getPartialResult();

  public void reset() {
    groups.clear();
    partialResult.clear();
  }

}
