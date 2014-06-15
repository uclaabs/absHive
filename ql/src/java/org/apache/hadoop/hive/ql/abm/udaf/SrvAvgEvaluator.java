package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.ValueListParser;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SrvAvgEvaluator extends SrvEvaluatorWithInstruction {


  private final List<String> columnName = new ArrayList<String>(Arrays.asList("Group","BaseSum", "BaseSsum", "BaseCnt"));

  private final List<ObjectInspector> objectInspectorType = new ArrayList<ObjectInspector>(Arrays.asList(
      (ObjectInspector)partialGroupOI,
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.javaIntObjectInspector));

  private final StructObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);

  private DoubleObjectInspector baseSumOI, baseSsumOI;
  private IntObjectInspector baseCntOI;
  private StructField sumField, ssumField, cntField;
  private SrvAvgComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if(m == Mode.PARTIAL2|| m == Mode.FINAL) {
      sumField = fields.get(1);
      ssumField = fields.get(2);
      cntField = fields.get(3);
      baseSumOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
      baseSsumOI = (DoubleObjectInspector) ssumField.getFieldObjectInspector();
      baseCntOI = (IntObjectInspector) cntField.getFieldObjectInspector();
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return partialOI;
    } else {
      compute = new SrvAvgComputation();
      return doubleListOI;
    }
  }

  protected static class SrvAvgAggregationBuffer extends SrvAggregationBuffer{

    public double baseSum = 0;
    public double baseSsum = 0;
    public int baseCnt = 0;

    public SrvAvgAggregationBuffer(ValueListParser inputParser) {
      super(inputParser);
    }

    @Override
    public void processBase(double value) {
      this.baseSum += value;
      this.baseSsum += (value * value);
      this.baseCnt += 1;
    }

    public void processPartialBase(double sum, double ssum, int cnt) {
      this.baseSum += sum;
      this.baseSsum += ssum;
      this.baseCnt += cnt;
    }

    @Override
    public Object getPartialResult() {
      super.addGroupToRet();
      ret.add(baseSum);
      ret.add(baseSsum);
      ret.add(baseCnt);
      return ret;
    }

    @Override
    public void reset() {
      super.reset();
      baseSum = baseSsum = 0;
      baseCnt = 0;
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new SrvAvgAggregationBuffer(this.valueListParser);
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((SrvAvgAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  protected void parseBaseInfo(SrvAggregationBuffer agg, Object partialRes) {
    Object sumObj = this.mergeInputOI.getStructFieldData(partialRes, sumField);
    Object ssumObj = this.mergeInputOI.getStructFieldData(partialRes, ssumField);
    Object cntObj = this.mergeInputOI.getStructFieldData(partialRes, cntField);
    double sum = this.baseSumOI.get(sumObj);
    double ssum = this.baseSsumOI.get(ssumObj);
    int cnt = this.baseCntOI.get(cntObj);
    ((SrvAvgAggregationBuffer)agg).processPartialBase(sum, ssum, cnt);
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    SrvAvgAggregationBuffer myagg = (SrvAvgAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
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
