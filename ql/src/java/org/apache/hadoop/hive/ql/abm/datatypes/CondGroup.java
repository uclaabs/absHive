package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.List;

public class CondGroup {
  
  int cnt;
  List<KeyWrapper> keys;
  List<List<RangeList>> ranges;
  
  public CondGroup()
  {
    cnt = -1;
    this.keys = new ArrayList<KeyWrapper>();
    this.ranges = new ArrayList<List<RangeList>>();
  }
  
  public void addGroup(int dimension, KeyWrapper keyList)
  {
    cnt ++;
    this.keys.add(keyList);
    List<RangeList> rangeMatrix  = new ArrayList<RangeList>();
    
    for(int i = 0; i < dimension; i ++)
      rangeMatrix.add(new RangeList());
    this.ranges.add(rangeMatrix);
  }
  
  public void addKeys(KeyWrapper keyList)
  {
    this.keys.add(keyList);
  }
  
  public void addRanges(List<RangeList> rangeMatrix)
  {
    this.ranges.add(rangeMatrix);
  }
  
  public List<KeyWrapper> getKeys()
  {
    return this.keys;
  }
  
  public List<List<RangeList>> getRanges()
  {
    return this.ranges;
  }
  
  public List<RangeList> getRangeMatrix()
  {
    return this.ranges.get(cnt);
  }
  
  public List<RangeList> getRangeMatrix(int i)
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
