package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.algebra.ComparisonTransform;
import org.apache.hadoop.hive.ql.abm.datatypes.Conditions;
import org.apache.hadoop.hive.ql.abm.datatypes.KeyWrapper;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;

public class KeyReader implements Serializable {

  private static final long serialVersionUID = 1L;

  private int[] gbys;
  private int[] cols;
  private PredicateType[] preds;
  private IntArrayList uniqueGbys;
  
  private transient List<RangeList> ranges;
  private transient IntArrayList idx = new IntArrayList();

  public KeyReader() {
  }

  public KeyReader(ComparisonTransform[] predicates) {
    // TODO: initialize uniqGbys & gbys & cols & preds
  }

  public void init(Conditions condition, IntArrayList[] groups, int[] numAggrs) {
    ranges = condition.getRanges();
    
    // get unique groupByOps
    IntOpenHashSet hSet = new IntOpenHashSet();
    for(int gby:gbys) {
      hSet.add(gby);
    }
    uniqueGbys = new IntArrayList(hSet);
    Collections.sort(uniqueGbys);
    
    // create a temporal Int2IntLinkedOpenHashMap for every group
    Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap> groupMaps = new Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap>();
    for(int gby: uniqueGbys) {
      groupMaps.put(gby, new Int2IntLinkedOpenHashMap());
    }
    
    int dimensions = gbys.length;
    KeyWrapper keys = condition.keys;
    int condGroupSize = keys.size() / dimensions;
    
    // fill in the groups and groupMaps
    for(int i = 0; i < dimensions; i ++) {
      // for every dimension, they have the same groupByOp
      int groupByOp = gbys[i];
      int colIdx = cols[i];
      Int2IntLinkedOpenHashMap hashMap = groupMaps.get(groupByOp);
      
      // for the ith dimension in every condGroup, update the hashMap and groups
      for(int j = 0; j < condGroupSize; j ++) {
        int offset = j * dimensions + i;
        int group = keys.getInt(offset);
        
        if(!hashMap.containsKey(group)) {
          hashMap.put(group, colIdx);
          groups[groupByOp].add(group);
        }
      }
    }
    
    // update the groupMaps to compute the offset
    int offset = 0;
    for(int gby: uniqueGbys) {
      int numAggr = numAggrs[gby];
      Int2IntLinkedOpenHashMap hashMap = groupMaps.get(gby);
      for(Map.Entry<Integer, Integer> entry: hashMap.entrySet()) {
        int key = entry.getKey();
        int value = entry.getValue() + offset;
        hashMap.put(key, value);
        offset += numAggr;
      }
    }
    
    // fill in the idx
    idx.clear();
    int keySize = keys.size();
    for(int i = 0; i < keySize; i ++) {
      int group = keys.getInt(i);
      int groupByOp = gbys[i%dimensions];
      idx.add(groupMaps.get(groupByOp).get(group));
    }
 
  }

  public int parse(double[] samples) {
    int left = 0;
    int right = ranges.get(0).size();

    int cur = 0;  // range
    int pos = 0;  // key
    
    while (true) {
      switch (preds[cur]) {
      case SINGLE_LESS_THAN:
        // double val = samples[idx.getInt(pos)]
        // left = findLargestValueSmallerThan(val, left, right)
        // right findSmallestValueLargerThanOrEqualTo(val, left, right)
        ++pos;
        break;

      case SINGLE_LESS_THAN_OR_EQUAL_TO:
        ++pos;
        break;

      case SINGLE_GREATER_THAN:
        ++pos;
        break;

      case SINGLE_GREATER_THAN_OR_EQUAL_TO:
        ++pos;
        break;

      case DOUBLE_LESS_THAN:
        pos += 2;
        break;

      case DOUBLE_LESS_THAN_OR_EQUAL_TO:
        pos += 2;
        break;

      case DOUBLE_GREATER_THAN:
        pos += 2;
        break;

      default: // case DOUBLE_GREATER_THAN_OR_EQUAL_TO:
        // TODO
        pos += 2;
      }

      if (left == right) {
        // TODO
        return left;
      } else if (left > right) {
        // TODO
        return -1;
      }

      ++cur;
      if (cur == preds.length) {
        cur = 0;
      }
    }
  }

}

enum PredicateType {
  SINGLE_LESS_THAN,
  SINGLE_LESS_THAN_OR_EQUAL_TO,
  SINGLE_GREATER_THAN,
  SINGLE_GREATER_THAN_OR_EQUAL_TO,
  DOUBLE_LESS_THAN,
  DOUBLE_LESS_THAN_OR_EQUAL_TO,
  DOUBLE_GREATER_THAN,
  DOUBLE_GREATER_THAN_OR_EQUAL_TO
}