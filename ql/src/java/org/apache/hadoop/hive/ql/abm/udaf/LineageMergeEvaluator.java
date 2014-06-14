package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class LineageMergeEvaluator extends GenericUDAFEvaluatorWithInstruction {

  private final static ListObjectInspector binaryListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
  private final static ListObjectInspector partialOI = ObjectInspectorFactory
      .getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector));

  private LineageInputParser parser = null;
  private ListObjectInspector listOI = null;

  private LineageComputation compute = null;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
      parser = new IntParser(parameters[0]);
    } else {
      listOI = (ListObjectInspector) parameters[0];
      parser = new IntArrayListParser(listOI.getListElementObjectInspector());
    }

    if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
      return partialOI;
    } else {
      compute = new LineageComputation();
      // return binaryListOI;
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }
  }

  private static class MyAggregationBuffer implements AggregationBuffer {

    Int2ObjectLinkedOpenHashMap<IntArrayList> groups = new Int2ObjectLinkedOpenHashMap<IntArrayList>();
    private final LineageInputParser parser;

    public MyAggregationBuffer(LineageInputParser _parser) {
      parser = _parser;
    }

    public void addTupleId(int key, Object o) {
      IntArrayList lineageList = groups.get(key);
      if (lineageList == null) {
        lineageList = new IntArrayList();
        groups.put(key, lineageList);
      }
      parser.parseInto(o, lineageList);
    }

    public Object getPartialResult() {
      return new ArrayList<IntArrayList>(groups.values());
    }

    public void reset() {
      groups.clear();
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new MyAggregationBuffer(parser);
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MyAggregationBuffer) agg).reset();
    compute.clear();
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (parameters[0] != null) {
      int key = ins.getGroupInstruction().getInt(0);
      if (key >= 0) {
        MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
        myagg.addTupleId(key, parameters[0]);
      }
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    return ((MyAggregationBuffer) agg).getPartialResult();
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    IntArrayList inst = ins.getGroupInstruction();

    int length = listOI.getListLength(partial); // Number of map entry
    for (int i = 0; i < length; i++) {
      int key = inst.getInt(i);
      myagg.addTupleId(key, listOI.getListElement(partial, i));
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    List<Merge> instructions = ins.getMergeInstruction();

    int i = 0;
    for (Map.Entry<Integer, IntArrayList> entry : myagg.groups.entrySet()) {
      compute.setGroupBitmap(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      ++i;
    }

    return compute.getFinalResult();
  }

}

interface LineageInputParser {
  abstract void parseInto(Object o, IntArrayList output);
}

class IntArrayListParser implements LineageInputParser {

  private final ListObjectInspector oi;
  private final IntObjectInspector eoi;

  public IntArrayListParser(ObjectInspector _oi) {
    oi = (ListObjectInspector) _oi;
    eoi = (IntObjectInspector) oi.getListElementObjectInspector();
  }

  @Override
  public void parseInto(Object o, IntArrayList output) {
    int length = oi.getListLength(o);
    for (int i = 0; i < length; ++i) {
      output.add(eoi.get(oi.getListElement(o, i)));
    }
  }

}

class IntParser implements LineageInputParser {

  private final IntObjectInspector oi;

  public IntParser(ObjectInspector _oi) {
    oi = (IntObjectInspector) _oi;
  }

  @Override
  public void parseInto(Object o, IntArrayList output) {
    output.add(oi.get(o));
  }

}
