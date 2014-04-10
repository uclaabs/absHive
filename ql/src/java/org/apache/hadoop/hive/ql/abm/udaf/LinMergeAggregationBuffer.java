package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

@SuppressWarnings("deprecation")
public class LinMergeAggregationBuffer implements AggregationBuffer
{
  //tupleMap is the internal representation, we convert it to bitmap at the last step
  protected TupleMap tupleMap = new TupleMap();

  //store the differences between bitmaps, not initialized yet
  protected List<IntArrayList> diffList = new ArrayList<IntArrayList>();

  public void initContainer() {
    tupleMap = new TupleMap();
    //keyMap = null;
    //valueArray = new ArrayList<DoubleArrayList>();
    diffList = new ArrayList<IntArrayList>();
  }



  //for mappers
  public IntArrayList mapperMerge(boolean needsCopy, int baseIndex, int tuple_key, DoubleArrayList value) {
    IntArrayList diff = diffList.get(baseIndex);
    if (needsCopy) {
      diff = diff.clone();
    }

    tupleMap.put(tuple_key, value);
    return diff;
  }

  public IntArrayList mapperMergeEmpty(boolean needsCopy, int baseIndex, int tuple_key, DoubleArrayList value) {
    IntArrayList diff = diffList.get(baseIndex);
    if (needsCopy) {
      diff = diff.clone();
    }
    diff.add(tuple_key);

    tupleMap.put(tuple_key, value);
    return diff;
  }

  public IntArrayList mapperCreate(int tuple_key, DoubleArrayList value) {
    IntArrayList diff = new IntArrayList();
    diff.addAll(tupleMap.keys);

    tupleMap.put(tuple_key, value);
    return diff;
  }



  //for reducers
  public IntArrayList reducerMerge(boolean needsCopy, int baseIndex, Iterator<Integer> rightDiff) {
    IntArrayList leftDiff = diffList.get(baseIndex);
    if (needsCopy) {
      leftDiff = leftDiff.clone();
    }

    while (rightDiff.hasNext()) {
      leftDiff.add(rightDiff.next());
    }

    return leftDiff;
  }

  public IntArrayList reducerMergeRightAll(boolean needsCopy, int baseIndex, Iterator<Integer> rightAll) {
    IntArrayList leftDiff = diffList.get(baseIndex);
    if (needsCopy) {
      leftDiff = leftDiff.clone();
    }

    while (rightAll.hasNext()) {
      leftDiff.add(rightAll.next());
    }

    return leftDiff;
  }

  public IntArrayList reducerMergeLeftAll(Iterator<Integer> rightDiff) {
    IntArrayList leftDiff = new IntArrayList();
    //notice: left all are reflected in current tupleMap.keys
    leftDiff.addAll(tupleMap.keys);

    while (rightDiff.hasNext()) {
      leftDiff.add(rightDiff.next());
    }
    return leftDiff;
  }






  private void sort() {
    //sort diff map
    tupleMap.sort();

    //sort diff list
    for (int i=0; i<diffList.size(); i++) {
      IntArrayListSorter sorter = new IntArrayListSorter(diffList.get(i));
      Arrays.quickSort(0, sorter.size(), sorter, sorter);
    }
  }

  public Object getMapperToReducerObject() {
    sort(); //sort according to the key order

    Object[] ret = new Object[3];
    ret[0] = tupleMap.getKeyObject();
    ret[1] = tupleMap.getValueObject();

    Object[] bitmapObjects = new Object[diffList.size()];
    for (int i=0; i<diffList.size(); i++) {
      bitmapObjects[i] = LinMergeUtil.getIntArrayListObject(diffList.get(i));
    }
    ret[2] = bitmapObjects;

    //tupleMap.clear();
    return ret;
  }

  public Object getOutputObject(Integer id) {
    sort(); //sort according to the key order

    Object[] ret = new Object[4];
    ret[0] = id;
    ret[1] = tupleMap.getKeyObject();
    ret[2] = tupleMap.getValueObject();

    Object[] bitmapObjects = new Object[diffList.size()];
    for (int i=0; i<diffList.size(); i++) {
      bitmapObjects[i] = LinMergeUtil.getIntArrayListObject(diffList.get(i));
    }
    ret[3] = bitmapObjects;

    //tupleMap.clear();
    return ret;
  }

}