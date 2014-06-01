package org.apache.hadoop.hive.ql.abm.datatypes;

import java.util.ArrayList;
import java.util.List;

public class CondGroup {

  private int cnt = -1;
  private final List<KeyWrapper> keys = new ArrayList<KeyWrapper>();;
  private final List<List<RangeList>> ranges = new ArrayList<List<RangeList>>();

  public void addGroup(int dimension, KeyWrapper keyList) {
    cnt++;
    keys.add(keyList);
    List<RangeList> rangeMatrix = new ArrayList<RangeList>();

    for (int i = 0; i < dimension; i++) {
      rangeMatrix.add(new RangeList());
    }
    ranges.add(rangeMatrix);
  }

  public void addKeys(KeyWrapper keyList) {
    keys.add(keyList);
  }

  public void addRanges(List<RangeList> rangeMatrix) {
    ranges.add(rangeMatrix);
  }

  public List<KeyWrapper> getKeys() {
    return keys;
  }

  public List<List<RangeList>> getRanges() {
    return ranges;
  }

  public List<RangeList> getRangeMatrix() {
    return ranges.get(cnt);
  }

  public List<RangeList> getRangeMatrix(int i) {
    return ranges.get(i);
  }

  public int getGroupNumber() {
    return cnt + 1;
  }

  public void clear() {
    keys.clear();
    ranges.clear();
  }

}
