package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

public class SrvCountEvaluator extends GenericUDAFEvaluatorWithInstruction {

  protected final ListObjectInspector doubleListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);

  protected final LongWritable ret = new LongWritable(0);
  protected LongObjectInspector partialResOI = null;

  private SrvCountComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if(m == Mode.PARTIAL2 || m == Mode.FINAL) {
      partialResOI = (LongObjectInspector) parameters[0];
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else {
      compute = new SrvCountComputation();
      return doubleListOI;
    }
  }

  protected static class MyAggregationBuffer implements AggregationBuffer {

    long baseCnt = 0;

    public void addBase(long cnt) {
      baseCnt += cnt;
    }

    public void reset() {
      baseCnt = 0;
    }

  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new MyAggregationBuffer();
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MyAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (ins.getGroupInstruction().size() > 0) {
      int instruction = ins.getGroupInstruction().getInt(0);
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

      if (instruction == -1) {
        myagg.addBase(1);
      }
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    ret.set(((MyAggregationBuffer) agg).baseCnt);
    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    myagg.addBase(partialResOI.get(partial));
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    compute.setTotalTupleNumber(N);
    compute.setCount(myagg.baseCnt);
    List<Merge> instructions = ins.getMergeInstruction();

    for (int i = 0; i < instructions.size(); i++) {
      compute.addNewGroup();
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();
  }

}
