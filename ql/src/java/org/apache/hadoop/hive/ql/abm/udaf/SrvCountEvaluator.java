package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class SrvCountEvaluator extends GenericUDAFEvaluatorWithInstruction {

  protected final ListObjectInspector doubleListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
  protected long tot = AbmUtilities.getTotalTupleNumber();
  private SrvCountComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    } else {
      compute = new SrvCountComputation();
      return doubleListOI;
    }
  }

  protected static class MyAggregationBuffer implements AggregationBuffer {

    int baseCnt = 0;

    public void addBase(int cnt) {
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
    return ((MyAggregationBuffer) agg).baseCnt;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    int partialCnt = ((IntWritable) partial).get();
    myagg.addBase(partialCnt);
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    compute.setCount(this.tot, myagg.baseCnt);
    List<Merge> instructions = ins.getMergeInstruction();

    for (int i = 0; i < instructions.size(); i++) {
      compute.addNewGroup();
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
    }

    return compute.getFinalResult();
  }

}
