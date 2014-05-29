package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineageMergeEvaluator extends GenericUDAFEvaluatorWithInstruction {
  
  private final IntObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  private final BinaryObjectInspector binaryOI = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
  private final ListObjectInspector binaryListOI = ObjectInspectorFactory.getStandardListObjectInspector(binaryOI);
  private final ListObjectInspector partialOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
  
  
  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);
    
    if (m == Mode.PARTIAL1||!(parameters[0] instanceof ListObjectInspector)) {
      return partialOI;
    } else {
     return binaryListOI;
    }
  }
  
  private static class MyAggregationBuffer implements AggregationBuffer {
    Map<Integer, IntArrayList> groups = new LinkedHashMap<Integer, IntArrayList>();
    
    public void reset() {
      groups.clear();
    }
  }
  
  protected EWAHCompressedBitmap parseBitmap(Object partialRes) {
    byte[] partialKeys = ((BytesWritable) partialRes).getBytes();

    EWAHCompressedBitmap bitMap = new EWAHCompressedBitmap();
    ByteArrayInputStream bis = new ByteArrayInputStream(partialKeys);

    try {
      bitMap.readExternal(new ObjectInputStream(bis));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return bitMap;
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
      
      // TODO fake instruction for testing
      ins.fakeIterate();
      int instruction = ins.getGroupInstruction().getInt(0);
      
      if(instruction >= 0){
        MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
        int tupleID = ((IntObjectInspector)this.intOI).get(parameters[0]);
        IntArrayList lineageList = myagg.groups.get(instruction);
        
        if(lineageList == null) {
          lineageList = new IntArrayList();
          lineageList.add(tupleID);
          myagg.groups.put(instruction, lineageList);
        } else {
          lineageList.add(tupleID);
        }
      }
    }
    
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    Object[] values = new Object[myagg.groups.size()];
    
    int i = 0;
    for(Map.Entry<Integer, IntArrayList> entry:myagg.groups.entrySet()) {
      values[i] = entry.getValue().toArray();
      i ++;
    }
    
    return values;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    
    if (!(partial instanceof LazyBinaryArray)) {
      throw new UDFArgumentException("LineageMerge: Unknown Data Type");
    }
    
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    LazyBinaryArray binaryValues = (LazyBinaryArray) partial;
    
    // TODO fake group instruction for testing
    IntArrayList instruction = ins.getGroupInstruction();
    
    int numEntries = binaryValues.getListLength(); // Number of map entry
    
    for (int i = 0; i < numEntries; i++) {

      LazyBinaryArray lazyIntArray = (LazyBinaryArray) binaryValues.getListElementObject(i);
      int key = instruction.getInt(i);
      IntArrayList currentList = myagg.groups.get(key);
      
      if(currentList == null) {
        currentList = new IntArrayList();
        myagg.groups.put(key, currentList);
      }
      
      for(int j = 0; j < lazyIntArray.getListLength(); j ++) {
        currentList.add(((IntWritable)lazyIntArray.getListElementObject(j)).get());
      }
      
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    LineageComputation compute = new LineageComputation();
    List<Merge> instructions = ins.getMergeInstruction();
    
    int i = 0;
    for(Map.Entry<Integer, IntArrayList> entry: myagg.groups.entrySet()) {
      
      compute.setGroupBitmap(entry.getValue());
      Merge merge = instructions.get(i);
      merge.enumerate(compute);
      i ++;
    }
    return compute.getFinalResult();
  }

}

  class IntListConverter implements IntComparator, Swapper {
  
  IntArrayList intList = null;
  
  public void setIntList(IntArrayList inputList) {
    intList = inputList;
  }

  @Override
  public int compare(Integer arg0, Integer arg1) {
    return Double.compare(intList.getInt(arg0), intList.getInt(arg1));
  }
  
  @Override
  public int compare(int arg0, int arg1) {
    return Double.compare(intList.getInt(arg0), intList.getInt(arg1));
  }

  @Override
  public void swap(int arg0, int arg1) {
    int tmp = intList.getInt(arg0);
    intList.set(arg0, intList.getInt(arg1));
    intList.set(arg1, tmp);
  }
  
  public void sort() {
    it.unimi.dsi.fastutil.Arrays.quickSort(0, intList.size(), this, this);
  }
  
  public EWAHCompressedBitmap getBitmap() {
    
    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
    for(int i = 0; i < intList.size(); i ++) {
      bitmap.set(intList.getInt(i));
    }

    return bitmap;    
  }
}
