package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class CondGroups {
  
  int cnt;
  List<List<Integer>> keys;
  List<List<List<ConditionRange>>> ranges;
  
  public CondGroups()
  {
    cnt = -1;
    this.keys = new ArrayList<List<Integer>>();
    this.ranges = new ArrayList<List<List<ConditionRange>>>();
  }
  
  public void addGroup(IntArrayList keyList)
  {
    cnt ++;
    List<Integer> keyArray = new ArrayList<Integer>(keyList);
    this.keys.add(keyArray);
    List<List<ConditionRange>> rangeMatrix  = new ArrayList<List<ConditionRange>>();
    for(int i = 0; i < keyList.size(); i ++)
      rangeMatrix.add(new ArrayList<ConditionRange>());
    this.ranges.add(rangeMatrix);
  }
  
  public void addKeys(List<Integer> keyList)
  {
    this.keys.add(keyList);
  }
  
  public void addRanges(List<List<ConditionRange>> rangeMatrix)
  {
    this.ranges.add(rangeMatrix);
  }
  
  public List<List<Integer>> getKeys()
  {
    return this.keys;
  }
  
  public List<List<List<ConditionRange>>> getRanges()
  {
    return this.ranges;
  }
  
  public List<List<ConditionRange>> getRangeMatrix()
  {
    return this.ranges.get(cnt);
  }
  
  public List<List<ConditionRange>> getRangeMatrix(int i)
  {
    return this.ranges.get(i);
  }
  
  public int getGroupNumber()
  {
    return this.cnt + 1;
  }
  
  public void clear()
  {
    this.keys.clear();
    this.ranges.clear();
  }

}
