package org.apache.hadoop.hive.ql.abm.udaf;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.abm.datatypes.CondList;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class DummyCondMergeEvaluator extends CondMergeEvaluator {
  
  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);
    
    // TODO
    ins = new Instruction();
    
    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      // partialTerminate() will be called
      return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, Arrays.asList(
          (ObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(
              ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)),
          ObjectInspectorFactory.getStandardListObjectInspector(
              ObjectInspectorFactory.getStandardListObjectInspector(
                  ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector))))
          );
    } else {
      // TODO remove later
      fakeFlags();
      return CondList.condListOI;
      
    }
    
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    ins.addGroupInstruction(-1);
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
  }

}
