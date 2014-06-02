package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class SrvAvgEvaluator extends SrvSumEvaluator {

  protected final IntObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  protected final List<String> avgColumnName = Arrays.asList("BaseSum", "BaseSsum", "BaseCnt",
      "Group");
  protected final List<ObjectInspector> avgObjectInspectorType = Arrays.asList(
      (ObjectInspector) doubleOI, (ObjectInspector) doubleOI, (ObjectInspector) intOI,
      (ObjectInspector) partialGroupOI);
  protected final StructObjectInspector avgPartialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(avgColumnName, avgObjectInspectorType);

  private SrvAvgComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      inputValueOI = (PrimitiveObjectInspector) parameters[0];
      return avgPartialOI;
    } else {
      compute = new SrvAvgComputation();
      return doubleListOI;
    }
  }

  protected static class MyAggregationBuffer implements AggregationBuffer {

    Map<Integer, DoubleArrayList> groups = new LinkedHashMap<Integer, DoubleArrayList>();
    List<DoubleArrayList> partialResult = new ArrayList<DoubleArrayList>();
    List<Object> ret = new ArrayList<Object>();
    double baseSum = 0;
    double baseSsum = 0;
    int baseCnt = 0;

    public void addBase(double value) {
      this.baseSum += value;
      this.baseSsum += (value * value);
      this.baseCnt += 1;
    }

    public void addBase(double partialSum, double partialSsum, int partialCnt) {
      baseSum += partialSum;
      baseSsum += partialSsum;
      baseCnt += partialCnt;
    }

    public Object getPartialResult() {

      ret.clear();
      partialResult.clear();
      for (Map.Entry<Integer, DoubleArrayList> entry : groups.entrySet()) {
        partialResult.add(entry.getValue());
      }
      ret.add(baseSum);
      ret.add(baseSsum);
      ret.add(baseCnt);
      ret.add(partialResult);
      return ret;
    }

    public void reset() {
      baseSum = baseSsum = 0;
      baseCnt = 0;
      groups.clear();
      partialResult.clear();
      ret.clear();
    }
  }

  @Override
  protected LazyBinaryArray parsePartialInput(AggregationBuffer agg, LazyBinaryStruct binaryStruct) {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    double partialSum = ((DoubleWritable) binaryStruct.getField(0)).get();
    double partialSsum = ((DoubleWritable) binaryStruct.getField(1)).get();
    int partialCnt = ((IntWritable) binaryStruct.getField(2)).get();
    myagg.addBase(partialSum, partialSsum, partialCnt);
    return (LazyBinaryArray) binaryStruct.getField(3);
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MyAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
    compute.setTotalNumber(tot);
    compute.setBase(myagg.baseSum, myagg.baseSsum, myagg.baseCnt);
    for (Map.Entry<Integer, DoubleArrayList> entry : myagg.groups.entrySet()) {

      compute.setCurrentList(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i++;
    }
    return compute.getFinalResult();

  }

}
