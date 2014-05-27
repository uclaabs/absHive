package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;


public class CondMergeEvaluator extends GenericUDAFEvaluator {

  private final ListObjectInspector keyArrayOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  private final ListObjectInspector keyObjOI = ObjectInspectorFactory.getStandardListObjectInspector(keyArrayOI);
  private final StructObjectInspector rangeOI = (StructObjectInspector) ConditionRange.conditionRangeInspector;
  private final ListObjectInspector rangeArrayOI =  ObjectInspectorFactory.getStandardListObjectInspector(rangeOI);
  private final ListObjectInspector rangeMatrixOI =  ObjectInspectorFactory.getStandardListObjectInspector(rangeArrayOI);
  private final ListObjectInspector rangeObjOI =  ObjectInspectorFactory.getStandardListObjectInspector(rangeMatrixOI);
  private final StructObjectInspector condOI = CondGroup.condGroupInspector;
  private final IntObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
//  private DoubleObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

  private final KeyWrapper key = new KeyWrapper();
  private final List<ConditionRange> inputRange = new ArrayList<ConditionRange>();
  protected Instruction ins = new Instruction();

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);
    return this.condOI;
  }

  static class MyAggregationBuffer implements AggregationBuffer {
    Map<IntArrayList, List<List<ConditionRange>>> groups;
    Map<IntArrayList, Integer> keyIndexes;
    int d;
  }
  
  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
    myagg.groups.clear();
    myagg.keyIndexes.clear();
    myagg.d = 0;
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    MyAggregationBuffer myagg = new MyAggregationBuffer();
    myagg.groups = new LinkedHashMap<IntArrayList, List<List<ConditionRange>>>();
    myagg.keyIndexes = new HashMap<IntArrayList, Integer>();
    myagg.d = 0;
    return myagg;
  }

  protected void updateRangeMatrix(List<List<ConditionRange>> rangeMatrix, LazyBinaryArray binaryMatrix, boolean flag) {
    for(int i = 0; i < binaryMatrix.getListLength(); i ++) {
      List<ConditionRange> newConds;
      if(flag) {
        newConds = new ArrayList<ConditionRange>();
      } else {
        newConds = rangeMatrix.get(i);
      }

      LazyBinaryArray lazyArray = (LazyBinaryArray) binaryMatrix.getListElementObject(i);
      for(int j = 0; j < lazyArray.getListLength(); j ++) {
        LazyBinaryStruct condObj = (LazyBinaryStruct) lazyArray.getListElementObject(j);
        newConds.add(new ConditionRange(condObj));
      }
      if(flag) {
        rangeMatrix.add(newConds);
      }
    }
  }

  protected void mergeBinaryStruct(MyAggregationBuffer myagg, Object partialRes) {
    LazyBinaryStruct binaryStruct = (LazyBinaryStruct) partialRes;
    LazyBinaryArray binaryKeys = (LazyBinaryArray) binaryStruct.getField(0);
    LazyBinaryArray binaryValues = (LazyBinaryArray) binaryStruct.getField(1);

    // binaryKeys.getListLength(): the number of map entry
    int entryNumber = binaryKeys.getListLength();
    for (int i = 0; i < entryNumber; i++) {
      // for every entry, the keyList and rangeMatrix
      LazyBinaryArray keyList = (LazyBinaryArray) binaryKeys.getListElementObject(i);
      LazyBinaryArray condRangeMatrix = (LazyBinaryArray) binaryValues.getListElementObject(i);

      key.newKey();
      for (int j = 0; j < keyList.getListLength(); j++) {
        key.add(((IntWritable) keyList.getListElementObject(j)).get());
      }

      if (myagg.d == 0) {
        myagg.d = key.size();
      }

      if (myagg.groups.containsKey(key)) {

        List<List<ConditionRange>> rangeGroup = myagg.groups.get(key);
        updateRangeMatrix(rangeGroup, condRangeMatrix, false);

        // 
        ins.setGroupInstruction(myagg.keyIndexes.get(key));
        
      } else {

        List<List<ConditionRange>> rangeGroup = new ArrayList<List<ConditionRange>>();
        updateRangeMatrix(rangeGroup, condRangeMatrix, true);
        
        myagg.keyIndexes.put(key.copyKey(), myagg.groups.size());
        // set instruction here
        ins.setGroupInstruction(myagg.groups.size());
        myagg.groups.put(key.copyKey(), rangeGroup);

        // TODO
      }
    }
  }



  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    
    if(parameters.length == 0) {
      //
      ins.setGroupInstruction(-1);
    }
    else if (parameters[0] != null) {
      /*
       * get the input Condition and ID
       */
      key.newKey();
      inputRange.clear();

      Object keysObj = this.condOI.getStructFieldData(parameters[0], this.condOI.getStructFieldRef("Keys"));
      Object rangesObj = this.condOI.getStructFieldData(parameters[0], this.condOI.getStructFieldRef("Values"));

      // assume there is only one key array in input
      Object keyObj = this.keyObjOI.getListElement(keysObj, 0);
      Object rangeMatrixObj = this.rangeObjOI.getListElement(rangesObj, 0);

      for (int i = 0; i < this.keyArrayOI.getListLength(keyObj); i++) {
        key.add(intOI.get(this.keyArrayOI.getListElement(keyObj, i)));
      }
      
      boolean baseFlag = true;
      for(int i = 0; i < this.rangeMatrixOI.getListLength(rangeMatrixObj); i ++) {
        Object rangeArrayObj = this.rangeMatrixOI.getListElement(rangeMatrixObj, i);
        ConditionRange newConditionRange = new ConditionRange(this.rangeArrayOI.getListElement(rangeArrayObj, 0));
        baseFlag = newConditionRange.isBase();
        inputRange.add(newConditionRange);
      }
      if(baseFlag){
        ins.setGroupInstruction(-1);
        return;
      }
      
      // put the tuples in input Condition List to different groups
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      if (myagg.groups.containsKey(key)) {

        List<List<ConditionRange>> rangeGroup = myagg.groups.get(key);
        for(int i = 0; i< inputRange.size(); i ++){
          rangeGroup.get(i).add(inputRange.get(i));
        }
        
        ins.setGroupInstruction(myagg.keyIndexes.get(key));
        
      } else {
        List<List<ConditionRange>> rangeGroup = new ArrayList<List<ConditionRange>>();

        for(int i = 0; i< inputRange.size(); i ++){
          List<ConditionRange> arrayRange = new ArrayList<ConditionRange>();
          arrayRange.add(inputRange.get(i));
          rangeGroup.add(arrayRange);
        }
        
        myagg.keyIndexes.put(key.copyKey(), myagg.groups.size());
        // set instruction here
        ins.setGroupInstruction(myagg.groups.size());
        myagg.groups.put(key.copyKey(), rangeGroup);
      }      
    }

//    System.out.println("Iterate Instruction:" + ins.getGroupInstruction());
  }

  protected void print(List<Integer> list) {
    System.out.print("Print List:");
    for (Integer number : list) {
      System.out.print(number + "\t");
    }
    System.out.println();
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    Object[] ret = new Object[2];
    Object[] keys = new Object[myagg.groups.size()];
    Object[] values = new Object[myagg.groups.size()];

    int i = 0;
    for (Map.Entry<IntArrayList,  List<List<ConditionRange>>> entry : myagg.groups.entrySet()) {

      keys[i] = entry.getKey().toArray();
      List<List<ConditionRange>> rangeGroup = entry.getValue();


      Object[] rangeMatrix = new Object[rangeGroup.size()];
      for(int j = 0; j < rangeGroup.size(); j ++) {
        Object[] rangeArray = new Object[rangeGroup.get(j).size()];
        List<ConditionRange> rangeList = rangeGroup.get(j);
        for(int k = 0 ; k < rangeList.size(); k ++) {
          rangeArray[k] = rangeList.get(k).toArray();
        }
        rangeMatrix[j] = rangeArray;
      }
      values[i] = rangeMatrix;
      i++;
    }
    ret[0] = keys;
    ret[1] = values;

    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException {
    if (partialRes != null) {
      MyAggregationBuffer myagg = (MyAggregationBuffer) agg;
      if (partialRes instanceof LazyBinaryStruct) {
        mergeBinaryStruct(myagg, partialRes);
      } else {
        throw new UDFArgumentException("CondMerge: Unknown Data Type");
      }
    }
    
//    System.out.println("Merge Instruction: " + ins.getGroupInstruction());
  }


  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {

    MyAggregationBuffer myagg = (MyAggregationBuffer) agg;

    CondGroup myCondGroup = new CondGroup();
    ConditionComputation condComputation = new ConditionComputation();
    condComputation.setCondGroup(myCondGroup, myagg.d);
    
    for (Map.Entry<IntArrayList, List<List<ConditionRange>>> entry : myagg.groups.entrySet()) {
      IntArrayList keyArray = entry.getKey();
      List<List<ConditionRange>> rangeMatrix = entry.getValue();
      condComputation.setFields(keyArray, rangeMatrix);

      Merge merge = new Merge();
      for(List<ConditionRange> rangeArray:rangeMatrix) {
        merge.addDimension(rangeArray);
      }
      
      ins.setMergeInstruction(merge);
      condComputation.setFlags(merge.getFlags());
      merge.enumerate(condComputation);
    }
//    System.out.println("Terminate Instruction Size:" + ins.getMergeInstruction().size());
    
    condComputation.unfold();
    
    return myCondGroup.toArray();
  }

}
