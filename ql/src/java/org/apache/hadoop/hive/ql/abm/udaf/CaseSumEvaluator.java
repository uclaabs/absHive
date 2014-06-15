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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CaseSumEvaluator extends SrvEvaluatorWithInstruction {

  private final List<String> columnName = new ArrayList<String>(Arrays.asList("Group", "BaseSum"));
  private final List<ObjectInspector> objectInspectorType =
      new ArrayList<ObjectInspector>(Arrays.asList(
          (ObjectInspector) partialGroupOI,
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
  private final StructObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);

  private DoubleObjectInspector baseSumOI;
  private StructField sumField;
  private CaseSumComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
      sumField = fields.get(1);
      baseSumOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return partialOI;
    } else {
      compute = new CaseSumComputation();
      return doubleListOI;
    }
  }

  protected static class CaseSumAggregationBuffer extends SrvAggregationBuffer {

    public double baseSum = 0;

    public CaseSumAggregationBuffer(ValueListParser inputParser) {
      super(inputParser);
    }

    @Override
    public void processBase(double value) {
      this.baseSum += value;
    }

    @Override
    public Object getPartialResult() {
      super.addGroupToRet();
      ret.add(baseSum);
      return ret;
    }

    @Override
    public void reset() {
      super.reset();
      baseSum = 0;
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new CaseSumAggregationBuffer(this.valueListParser);
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((CaseSumAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    CaseSumAggregationBuffer myagg = (CaseSumAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
    compute.setBase(myagg.baseSum);
    for (Map.Entry<Integer, DoubleArrayList> entry : myagg.groups.entrySet()) {
      compute.setCurrentList(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i++;
    }
    return compute.getFinalResult();
  }

  @Override
  protected void parseBaseInfo(SrvAggregationBuffer agg, Object partialRes) {
    Object sumObj = this.mergeInputOI.getStructFieldData(partialRes, this.sumField);
    double sum = this.baseSumOI.get(sumObj);
    ((CaseSumAggregationBuffer) agg).processBase(sum);

  }

}
