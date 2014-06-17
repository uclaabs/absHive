package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;

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

  public void init(SrvTuple tuple, IntArrayList[] groups) {
    ranges = tuple.range;

    // dedup
    Int2IntLinkedOpenHashMap[] allGroupIds = new Int2IntLinkedOpenHashMap[uniqs.length];
    for (int i = 0; i < uniqs.length; ++i) {
      allGroupIds[i] = new Int2IntLinkedOpenHashMap();
      groups[i].clear();
    }

    IntArrayList keys = tuple.key;
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
    double value;

    int left = 0;
    int right = ranges.get(0).size();
    int[] tmpBound = {left, right};
    int index;

    int keyIdx = 0;
    int rangeIdx = 0;
    int predIdx = 0;

    while (true) {
      RangeList range = ranges.get(rangeIdx++);

      switch (preds[predIdx++]) {
      case SINGLE_LESS_THAN:
        value = samples[idx.getInt(keyIdx)];
        index = firstLte(value, range, left, right);
        if (range.getDouble(index) > value) {
          right = index;
          left = firstLte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstLte(range.getDouble(right), range, left, right);
        }
        ++keyIdx;
        break;

      case SINGLE_LESS_THAN_OR_EQUAL_TO:
        value = samples[idx.getInt(keyIdx)];
        index = firstLt(value, range, left, right);
        if (index == -1) {
          right = index;
          left = firstLte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstLte(range.getDouble(right), range, left, right);
        }
        ++keyIdx;
        break;

      case SINGLE_GREATER_THAN:
        value = samples[idx.getInt(keyIdx)];
        index = firstGte(value, range, left, right);
        if (index == -1) {
          right = index;
          left = firstGte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstGte(range.getDouble(right), range, left, right);
        }
        ++keyIdx;
        break;

      case SINGLE_GREATER_THAN_OR_EQUAL_TO:
        value = samples[idx.getInt(keyIdx)];
        index = firstGt(value, range, left, right);
        if (index == -1) {
          right = index;
          left = firstGte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstGte(range.getDouble(right), range, left, right);
        }
        ++keyIdx;
        break;

      case DOUBLE_LESS_THAN:
        value = samples[idx.getInt(keyIdx)] - samples[idx.getInt(keyIdx + 1)];
        index = firstLte(value, range, left, right);
        if (range.getDouble(index) > value) {
          right = index;
          left = firstLte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstLte(range.getDouble(right), range, left, right);
        }
        keyIdx += 2;
        break;

      case DOUBLE_LESS_THAN_OR_EQUAL_TO:
        value = samples[idx.getInt(keyIdx)] - samples[idx.getInt(keyIdx + 1)];
        index = firstLt(value, range, left, right);
        if (index == -1) {
          right = index;
          left = firstLte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstLte(range.getDouble(right), range, left, right);
        }
        keyIdx += 2;
        break;

      case DOUBLE_GREATER_THAN:
        value = samples[idx.getInt(keyIdx)] - samples[idx.getInt(keyIdx + 1)];
        index = firstGte(value, range, left, right);
        if (index == -1) {
          right = index;
          left = firstGte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstGte(range.getDouble(right), range, left, right);
        }
        keyIdx += 2;
        break;

      default: // case DOUBLE_GREATER_THAN_OR_EQUAL_TO
        value = samples[idx.getInt(keyIdx)] - samples[idx.getInt(keyIdx + 1)];
        index = firstGt(value, range, left, right);
        if (index == -1) {
          right = index;
          left = firstGte(range.getDouble(right), range, left, right);
        } else if (index == left) {
          left = right = range.size();
        } else {
          right = index - 1;
          left = firstGte(range.getDouble(right), range, left, right);
        }
        keyIdx += 2;
      }

      left = tmpBound[0];
      right = tmpBound[1];

      if (left == right) {
        return left;
      }

      if (predIdx == preds.length) {
        predIdx = 0;
      }
    }
  }

  private int firstLt(double value, RangeList range, int left, int right) {
    while (left < right) {
      int mid = (left + right) / 2;
      if (value > range.getDouble(mid)) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    if (value > range.getDouble(left)) {
      return left;
    } else {
      return -1;
    }
  }

  private int firstLte(double value, RangeList range, int left, int right) {
    while (left < right) {
      int mid = (left + right) / 2;
      if (value >= range.getDouble(mid)) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    if (value >= range.getDouble(left)) {
      return left;
    } else {
      return -1;
    }
  }

  private int firstGte(double value, RangeList range, int left, int right) {
    while (left < right) {
      int mid = (left + right) / 2;
      if (value <= range.getDouble(mid)) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }

    if (value <= range.getDouble(left)) {
      return left;
    } else {
      return -1;
    }
  }

  private int firstGt(double value, RangeList range, int left, int right) {
    while (left < right) {
      int mid = (left + right) / 2;
      if (value < range.getDouble(mid)) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    if (value < range.getDouble(left)) {
      return left;
    } else {
      return -1;
    }
  }

}
