package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.ValueListParser;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;


public abstract class SrvEvaluatorWithInstruction extends GenericUDAFEvaluatorWithInstruction{

  protected PrimitiveObjectInspector inputValueOI = null;
  protected StructObjectInspector mergeInputOI;
  protected List<? extends StructField> fields;
  protected ListObjectInspector valueGroupOI;
  protected ValueListParser valueListParser;
  protected StructField valueGroupField;

  // for final output
  protected final ListObjectInspector doubleListOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
  protected final ListObjectInspector partialGroupOI = ObjectInspectorFactory.getStandardListObjectInspector(doubleListOI);

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
      inputValueOI = (PrimitiveObjectInspector) parameters[0];
    } else {
      mergeInputOI = (StructObjectInspector) parameters[0];
      fields = mergeInputOI.getAllStructFieldRefs();
      valueGroupField = fields.get(0);
      valueGroupOI = (ListObjectInspector) valueGroupField.getFieldObjectInspector();
      valueListParser = new ValueListParser(valueGroupOI.getListElementObjectInspector());
    }
    return null;
  }


  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (parameters[0] != null) {
      int instruction = ins.getGroupInstruction().getInt(0);
      SrvAggregationBuffer myagg = (SrvAggregationBuffer) agg;
      double value = PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputValueOI);
      myagg.addValue(instruction, value);
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    SrvAggregationBuffer myagg = (SrvAggregationBuffer) agg;
    return myagg.getPartialResult();
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {

    SrvAggregationBuffer myagg = (SrvAggregationBuffer) agg;
    Object valueGroupObj = this.mergeInputOI.getStructFieldData(partialRes, this.valueGroupField);
    IntArrayList instruction = ins.getGroupInstruction();

    for (int i = 0; i < this.valueGroupOI.getListLength(valueGroupObj); i++) {
      Object valueListObj = this.valueGroupOI.getListElement(valueGroupObj, i);
      myagg.addValueList(instruction.getInt(i), valueListObj);
    }

    parseBaseInfo(myagg, partialRes);
  }

  protected abstract void parseBaseInfo(SrvAggregationBuffer agg, Object partialRes);




}
