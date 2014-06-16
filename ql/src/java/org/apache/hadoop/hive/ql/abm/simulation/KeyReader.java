package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.Conditions;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;

public class KeyReader {

  private final int[] uniqs;
  private final int[] numAggrs;

  private final int[] gbys;
  private final int[] cols;

  private final PredicateType[] preds;

  private List<RangeList> ranges = null;
  private final IntArrayList idx = new IntArrayList();

  public KeyReader(List<Integer> gbyIds, int[] numAggrs,
      List<Integer> gbyIdsInPreds, List<Integer> colsInPreds, List<PredicateType> predTypes) {
    // Initialize basic information
    int numUniq = gbyIds.size();
    uniqs = new int[numUniq];
    Int2IntOpenHashMap tmpMap = new Int2IntOpenHashMap();
    for (int i = 0; i < numUniq; ++i) {
      int id = gbyIds.get(i);
      uniqs[i] = id;
      tmpMap.put(id, tmpMap.size());
    }

    this.numAggrs = numAggrs;

    // Initialize key-related fields
    int numKeys = gbyIdsInPreds.size();
    gbys = new int[numKeys];
    cols = new int[numKeys];
    for (int i = 0; i < numKeys; ++i) {
      int idx = tmpMap.get(gbyIdsInPreds.get(i));
      gbys[i] = idx;
      int col = colsInPreds.get(i);
      cols[i] = (col == -1) ? numAggrs[idx] : col;

    }

    // Initialize predicate-related fields
    preds = predTypes.toArray(new PredicateType[predTypes.size()]);
  }

  public void init(Conditions condition, IntArrayList[] groups) {
    ranges = condition.getRanges();

    // dedup
    Int2IntLinkedOpenHashMap[] allGroupIds = new Int2IntLinkedOpenHashMap[uniqs.length];
    for (int i = 0; i < uniqs.length; ++i) {
      allGroupIds[i] = new Int2IntLinkedOpenHashMap();
      groups[i].clear();
    }

    IntArrayList keys = condition.keys;
    int numKeys = keys.size();
    for (int i = 0, j = 0; i < numKeys; ++i) {
      int idx = gbys[j++];
      Int2IntLinkedOpenHashMap tmp = allGroupIds[idx];
      int groupId = keys.getInt(i);
      if (!tmp.containsKey(groupId)) {
        tmp.put(groupId, tmp.size());
        groups[idx].add(groupId);
      }
      if (j == gbys.length) {
        j = 0;
      }
    }

    // fill in idx
    int[] offset = new int[uniqs.length];
    int cum = 0;
    for (int i = 0; i < uniqs.length; ++i) {
      offset[i] = cum;
      cum += allGroupIds[i].size() * numAggrs[i];
    }

    idx.clear();
    for (int i = 0, j = 0; i < numKeys; i++) {
      int gby = gbys[j++];
      int groupId = keys.getInt(i);
      idx.add(offset[gby] + allGroupIds[gby].get(groupId) * numAggrs[gby] + cols[i]);
      if (j == gbys.length) {
        j = 0;
      }
    }
  }

  public int parse(double[] samples) {
    int left = 0;
    int right = ranges.get(0).size();

    int[] tmpBound = {left, right};
    int cur = 0;
    int pos = 0;

    double valx, valy, value;

    while (true) {
      valx = samples[idx.getInt(pos)];
      RangeList currentRange = ranges.get(cur);

      switch (preds[cur % preds.length]) {
      case SINGLE_LESS_THAN:
        conditionLessThan(tmpBound, left, right, currentRange, valx);
        ++pos;
        break;

      case SINGLE_LESS_THAN_OR_EQUAL_TO:
        conditionLessEqualThan(tmpBound, left, right, currentRange, valx);
        ++pos;
        break;

      case SINGLE_GREATER_THAN:
        conditionGreaterThan(tmpBound, left, right, currentRange, valx);
        ++pos;
        break;

      case SINGLE_GREATER_THAN_OR_EQUAL_TO:
        conditionGreaterEqualThan(tmpBound, left, right, currentRange, valx);
        ++pos;
        break;

      case DOUBLE_LESS_THAN:
        valy = samples[idx.getInt(pos + 1)];
        value = valx - valy;
        conditionLessThan(tmpBound, left, right, currentRange, value);
        pos += 2;
        break;

      case DOUBLE_LESS_THAN_OR_EQUAL_TO:
        valy = samples[idx.getInt(pos + 1)];
        value = valx - valy;
        conditionLessThan(tmpBound, left, right, currentRange, value);
        pos += 2;
        break;

      case DOUBLE_GREATER_THAN:
        valy = samples[idx.getInt(pos + 1)];
        value = valx - valy;
        conditionGreaterThan(tmpBound, left, right, currentRange, value);
        pos += 2;
        break;

      default:
        valy = samples[idx.getInt(pos + 1)];
        value = valx - valy;
        conditionGreaterEqualThan(tmpBound, left, right, currentRange, value);
        pos += 2;
      }

      left = tmpBound[0];
      right = tmpBound[1];

      if (left == right) {
        return left;
      }

      ++cur;
    }
  }


  private int lessThan(int left, int right, RangeList range, double value) {
    while (right > left) {
      int midPos = (left + right) / 2;
      if (range.getDouble(midPos) < value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }

  private int lessEqualThan(int left, int right, RangeList range, double value) {
    while (right > left) {
      int midPos = (left + right) / 2;
      if (range.getDouble(midPos) <= value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }


  private int greaterEqualThan(int left, int right, RangeList range, double value) {
    while (right > left) {
      int midPos = (left + right) / 2;
      if (range.getDouble(midPos) >= value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }

  private int greaterThan(int left, int right, RangeList range, double value) {
    while (right > left) {
      int midPos = (left + right) / 2;
      if (range.getDouble(midPos) > value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }

  private void conditionGreaterEqualThan(int[] bound, int left, int right, RangeList range,
      double value) {

    int index = greaterThan(left, right, range, value);
    if (range.getDouble(index) <= value) {
      bound[1] = index;
      bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if (index == left) {
        bound[0] = range.size();
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
  }

  private void conditionGreaterThan(int[] bound, int left, int right, RangeList range, double value) {

    int index = greaterEqualThan(left, right, range, value);
    if (range.getDouble(index) < value) {
      bound[1] = index;
      bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if (index == left) {
        bound[0] = range.size();
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
  }

  private void conditionLessEqualThan(int[] bound, int left, int right, RangeList range,
      double value) {

    int index = lessThan(left, right, range, value);
    if (range.getDouble(index) >= value) {
      bound[1] = index;
      bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if (index == left) {
        bound[0] = range.size();
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
  }

  private void conditionLessThan(int[] bound, int left, int right, RangeList range, double value) {

    int index = lessEqualThan(left, right, range, value);
    if (range.getDouble(index) > value) {
      bound[1] = index;
      bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if (index == left) {
        bound[0] = range.size();
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
  }

}
