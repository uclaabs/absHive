package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.googlecode.javaewah.EWAHCompressedBitmap;

@SuppressWarnings("deprecation")
public class LinMergeEvaluator extends GenericUDAFInstructionSetEvaluator {

  protected PrimitiveObjectInspector inputKeyOI;
  protected PrimitiveObjectInspector[] inputValueOI;
  protected StandardMapObjectInspector loi;
  protected StructObjectInspector outputOI;
  protected StructObjectInspector partialOutputOI;
  protected StructObjectInspector internalMergeOI;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
    super.init(m, parameters);
    //id: Integer
    //keys: bitmap
    //values: list<list<double>>
    //diffs: list<bitmap>

    int parameterSize = parameters.length;
    if (m == Mode.PARTIAL1)
    {
      inputKeyOI = (PrimitiveObjectInspector) parameters[0];
      for(int i = 1; i < parameterSize; i++)
      {
        inputValueOI[i] = (PrimitiveObjectInspector) parameters[i];
      }
      partialOutputOI = LinMergeUtil.getPartialOutputOI();
      return partialOutputOI;
    }
    else {
      if (!(parameters[0] instanceof StructObjectInspector))
      {
        inputKeyOI = (PrimitiveObjectInspector) parameters[0];
        for(int i = 1; i < parameterSize; i++)
        {
          inputValueOI[i] = (PrimitiveObjectInspector) parameters[i];
        }
        partialOutputOI =  LinMergeUtil.getPartialOutputOI();
        return partialOutputOI;
      }
      else
      {
        internalMergeOI = (StructObjectInspector) parameters[0];
        outputOI =	LinMergeUtil.getOutputOI();
        return outputOI;
      }
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException
  {
    LinMergeAggregationBuffer myagg = (LinMergeAggregationBuffer)agg;
    myagg.initContainer();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException
  {
    LinMergeAggregationBuffer ret = new LinMergeAggregationBuffer();
    ret.initContainer();
    return ret;
  }

  /**
   * Decides whether we need to copy the previous diff or not,
   * @param ins
   * @param index
   * @return
   */
  private boolean needsCopy(int index) {
    //look ahead to decide needs copying or not
    if (index + 1 < instruction.size()) {
      return instruction.getBase(index) == instruction.getBase(index + 1);
    }

    //the last ins
    return false;
  }

  private void processInstructionForMapper(LinMergeAggregationBuffer myagg, int key, DoubleArrayList value) {
    List<IntArrayList> tmpDiffList = new ArrayList<IntArrayList>();

    /**
     * Example. instruction sequence:
     * 1,0 -> 1,-1 -> 2,-1 -> 2,0 -> 3,0 -> 4,0 -> ...
     */
    for (int i=0; i<instruction.size(); i++) {
      int baseIndex = instruction.getBase(i);
      int deltaIndex = instruction.getDelta(i);

      assert(baseIndex < myagg.diffList.size());
      assert((deltaIndex == 0) || (deltaIndex == -1));

      IntArrayList list = null;
      if (baseIndex != -1) {
        if (deltaIndex == 0) {
          list = myagg.mapperMerge(needsCopy(baseIndex), baseIndex, key, value);
        }
        else {
          list = myagg.mapperMergeEmpty(needsCopy(baseIndex), baseIndex, key, value);
        }
      }
      else {
        if (deltaIndex == 0) {
          list = myagg.mapperCreate(key, value);
        }
        else {
          assert(false);
        }
      }

      tmpDiffList.add(list);
    }

    myagg.diffList = tmpDiffList;
  }

  private void processInstructionForReducer(LinMergeAggregationBuffer myagg, EWAHCompressedBitmap rightKeys, List<DoubleArrayList> rightValues, List<EWAHCompressedBitmap> rightDiffList) {
    //merge diff list
    List<IntArrayList> tmpDiffList = new ArrayList<IntArrayList>();
    /**
     * Example. instruction sequence:
     * 1,2 -> 1,3 -> 2,2 -> 2,3 -> 3,3 -> 4,2 -> ...
     */
    for (int i=0; i<instruction.size(); i++) {
      int baseIndex = instruction.getBase(i);
      int deltaIndex = instruction.getDelta(i);

      assert(baseIndex < myagg.diffList.size());
      assert(deltaIndex < rightDiffList.size());

      IntArrayList list = null;
      if (baseIndex != -1) {
        if (deltaIndex != -1) {
          list = myagg.reducerMerge(needsCopy(baseIndex), baseIndex, rightDiffList.get(deltaIndex).iterator());
        }
        else {
          list = myagg.reducerMergeRightAll(needsCopy(baseIndex), baseIndex, rightKeys.iterator());
        }
      }
      else {
        if (deltaIndex != -1) {
          list = myagg.reducerMergeLeftAll(rightDiffList.get(deltaIndex).iterator());
        }
        else {
          assert(false);
        }
      }

      tmpDiffList.add(list);
    }

    myagg.diffList = tmpDiffList;

    //merge tuple list
    Iterator<Integer> iter = rightKeys.iterator();
    while (iter.hasNext()) {
      myagg.tupleMap.keys.add(iter.next());
    }
    myagg.tupleMap.values.addAll(rightValues);
  }

  /**
   * Mapper: iterate is called for every satisfied tuple
   *
   * At this time, the AggregationBuffer only contains the TupleMap.
   */
  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    //assert (parameters.length == 2);
    if(parameters[0] == null) {
      return;
    }

    int key = ((IntObjectInspector)inputKeyOI).get(parameters[0]);
    DoubleArrayList value = new DoubleArrayList();

    //currently only deals with one value
    for (int i = 1; i < parameters.length; i++) {
      double v = PrimitiveObjectInspectorUtils.getDouble(parameters[i], inputValueOI[i]);
      value.add(v);
    }

    LinMergeAggregationBuffer myagg = (LinMergeAggregationBuffer) agg;
    processInstructionForMapper(myagg, key, value);
  }

  /**
   * Mapper: This will be called after all iterate work is done
   */
  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    LinMergeAggregationBuffer myagg = (LinMergeAggregationBuffer) agg;
    return myagg.getMapperToReducerObject();
  }

  /**
   * Reducer: merge partial results to the AggregationBuffer
   */
  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
    if(partialRes == null) {
      return;
    }
    EWAHCompressedBitmap keys = LinMergeUtil.getKeysFrom(partialRes, internalMergeOI);
    List<DoubleArrayList> values = LinMergeUtil.getValuesFrom(partialRes, internalMergeOI);
    List<EWAHCompressedBitmap> diffList = LinMergeUtil.getDiffsFrom(partialRes, internalMergeOI);

    processInstructionForReducer((LinMergeAggregationBuffer) agg, keys, values, diffList);
  }

  /**
   * Reducer: terminate will be called after all merge work is done
   */
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    LinMergeAggregationBuffer myagg = (LinMergeAggregationBuffer) agg;
    return myagg.getOutputObject(id);
  }
}
