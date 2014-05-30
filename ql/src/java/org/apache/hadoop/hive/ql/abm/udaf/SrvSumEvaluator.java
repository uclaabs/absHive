package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class SrvSumEvaluator extends GenericUDAFEvaluatorWithInstruction {

  protected final DoubleObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  protected final ListObjectInspector doubleListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(doubleOI);
  protected final ListObjectInspector partialGroupOI = ObjectInspectorFactory
      .getStandardListObjectInspector(doubleListOI);

  protected final List<String> columnName = Arrays.asList("BaseSum", "BaseSsum", "Group");
  protected final List<ObjectInspector> objectInspectorType = Arrays.asList(
      (ObjectInspector) doubleOI, (ObjectInspector) doubleOI, (ObjectInspector) partialGroupOI);
  protected final StructObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);


  protected PrimitiveObjectInspector inputValueOI = null;

  // TODO replace fake one with abm.util.tot
  protected int tot = AbmUtilities.getTotalTupleNumber();

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || !(parameters[0] instanceof ListObjectInspector)) {

      inputValueOI = (PrimitiveObjectInspector) parameters[0];
      return partialOI;
    } else {
      return doubleListOI;
    }
  }

  protected static class MyAggregationBuffer implements AggregationBuffer {
    Map<Integer, DoubleArrayList> groups = new LinkedHashMap<Integer, DoubleArrayList>();
    List<DoubleArrayList> partialResult = new ArrayList<DoubleArrayList>();
    double baseSum = 0;
    double baseSsum = 0;

    public void addBase(double value) {
      this.baseSum += value;
      this.baseSsum += (value * value);
    }

    public void addBase(double partialSum, double partialSsum) {
      baseSum += partialSum;
      baseSsum += partialSsum;
    }

    public Object getPartialResult() {
      // TODO: reuse
      Object[] ret = new Object[2];
      partialResult.clear();
      for (Map.Entry<Integer, DoubleArrayList> entry : groups.entrySet()) {
        partialResult.add(entry.getValue());
      }
      ret[0] = baseSum;
      ret[1] = baseSsum;
      ret[2] = partialResult;
      return ret;
    }

    public void reset() {
      baseSum = baseSsum = 0;
      groups.clear();
      partialResult.clear();
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new MyAggregationBuffer();
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    myagg.reset();
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (parameters[0] != null) {

      int instruction = ins.getGroupInstruction().getInt(0);

      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      double value = PrimitiveObjectInspectorUtils.getDouble(parameters, inputValueOI);

      if (instruction >= 0) {
        DoubleArrayList lineageList = myagg.groups.get(instruction);

        if (lineageList == null) {
          lineageList = new DoubleArrayList();
          lineageList.add(value);
          myagg.groups.put(instruction, lineageList);
        } else {
          lineageList.add(value);
        }
      } else {
        myagg.addBase(value);
      }
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    return myagg.getPartialResult();
  }

  protected LazyBinaryArray parsePartialInput(AggregationBuffer agg, LazyBinaryStruct binaryStruct) {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    double partialSum = ((DoubleWritable) binaryStruct.getField(0)).get();
    double partialSsum = ((DoubleWritable) binaryStruct.getField(1)).get();
    myagg.addBase(partialSum, partialSsum);
    return (LazyBinaryArray) binaryStruct.getField(2);
  }


  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    if (!(partial instanceof LazyBinaryStruct)) {
      throw new UDFArgumentException("SrvSum: Unknown Data Type");
    }

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    LazyBinaryStruct binaryStruct = (LazyBinaryStruct) partial;
    LazyBinaryArray binaryValues = parsePartialInput(agg, binaryStruct);

    IntArrayList instruction = ins.getGroupInstruction();

    int numEntries = binaryValues.getListLength(); // Number of map entry
    for (int i = 0; i < numEntries; i++) {
      LazyBinaryArray lazyIntArray = (LazyBinaryArray) binaryValues.getListElementObject(i);
      int key = instruction.getInt(i);
      DoubleArrayList currentList = myagg.groups.get(key);

      if (currentList == null) {
        currentList = new DoubleArrayList();
        myagg.groups.put(key, currentList);
      }

      for (int j = 0; j < lazyIntArray.getListLength(); j++) {
        currentList.add(((DoubleWritable) lazyIntArray.getListElementObject(j)).get());
      }
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    // TODO: reuse
    SrvSumComputation compute = new SrvSumComputation();
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
    compute.setTotalNumber(tot);
    compute.setBase(myagg.baseSum, myagg.baseSsum);
    for (Map.Entry<Integer, DoubleArrayList> entry : myagg.groups.entrySet()) {

      compute.setCurrentList(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i++;
    }
    return compute.getFinalResult();
  }

}
