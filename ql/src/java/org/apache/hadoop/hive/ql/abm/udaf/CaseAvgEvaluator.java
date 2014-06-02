package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

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

public class CaseAvgEvaluator  extends SrvEvaluatorWithInstruction {


  private final List<String> columnName = Arrays.asList("Group","BaseSum", "BaseCnt");
  
  private final List<ObjectInspector> objectInspectorType = Arrays.asList(
      (ObjectInspector)partialGroupOI,  
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector, 
      PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  
  private final StructObjectInspector partialOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);
  
  private DoubleObjectInspector baseSumOI;
  private IntObjectInspector baseCntOI;
  private StructField sumField, cntField;
  private CaseAvgComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);
    
    if(m == Mode.PARTIAL2|| m == Mode.FINAL) {
      sumField = fields.get(1);
      cntField = fields.get(2);
      baseSumOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
      baseCntOI = (IntObjectInspector) cntField.getFieldObjectInspector();
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return partialOI;
    } else {
      compute = new CaseAvgComputation();
      return doubleListOI;
    }
  }

  protected static class CaseAvgAggregationBuffer extends SrvAggregationBuffer{
    
    public double baseSum = 0;
    public int baseCnt = 0;
    
    public CaseAvgAggregationBuffer(ValueListParser inputParser) {
      super(inputParser);
    }

    @Override
    public void processBase(double value) {
      this.baseSum += value;
      this.baseCnt += 1;
    }
    
    public void processPartialBase(double sum,  int cnt) {
      this.baseSum += sum;
      this.baseCnt += cnt;
    }

    @Override
    public Object getPartialResult() {
      super.addGroupToRet();
      ret.add(baseSum);
      ret.add(baseCnt);
      return ret;
    }
    
    @Override
    public void reset() {
      super.reset();
      baseSum = 0;
      baseCnt = 0;
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new CaseAvgAggregationBuffer(this.valueListParser);
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((CaseAvgAggregationBuffer) agg).reset();
    compute.clear();
  }
  
  @Override
  protected void parseBaseInfo(SrvAggregationBuffer agg, Object partialRes) {
    Object sumObj = this.mergeInputOI.getStructFieldData(partialRes, sumField);
    Object cntObj = this.mergeInputOI.getStructFieldData(partialRes, cntField);
    double sum = this.baseSumOI.get(sumObj);
    int cnt = this.baseCntOI.get(cntObj);
    ((CaseAvgAggregationBuffer)agg).processPartialBase(sum, cnt);
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    CaseAvgAggregationBuffer myagg = (CaseAvgAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
    compute.setBase(myagg.baseSum,  myagg.baseCnt);
    for (Map.Entry<Integer, DoubleArrayList> entry : myagg.groups.entrySet()) {

      compute.setCurrentList(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i++;
    }
    return compute.getFinalResult();

  }

}
