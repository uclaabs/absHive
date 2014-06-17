package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;

public class KeyReader {

  private final int[] numAggrs;
  private final TupleMap[] dict;

  private final int[] gbys;
  private final int[] cols;

  private final PredicateType[] preds;

  private final TupleList[] targetTuples;
  private final IntArrayList condIds;

  public KeyReader(List<Integer> gbyIds, int[] numAggrs,
      List<Integer> gbyIdsInPreds, List<Integer> colsInPreds, List<PredicateType> predTypes,
      TupleMap[] srvs) {
    // Initialize basic information
    int numUniq = gbyIds.size();
    dict = new TupleMap[numUniq];
    Int2IntOpenHashMap tmpMap = new Int2IntOpenHashMap();
    for (int i = 0; i < numUniq; ++i) {
      int id = gbyIds.get(i);
      dict[i] = srvs[id];
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

    // Initialize simulation-related fields
    targetTuples = new TupleList[numUniq];
    condIds = new IntArrayList();
  }

  public int init(IntArrayList[] target, IntArrayList[] dependent) {
    Int2IntLinkedOpenHashMap[] allGroupIds = new Int2IntLinkedOpenHashMap[numAggrs.length];
    for (int i = 0; i < numAggrs.length; ++i) {
      allGroupIds[i] = new Int2IntLinkedOpenHashMap();
      targetTuples[i].clear();
      dependent[i].clear();
    }

    for (int ind = 0; ind < dict.length; ++ind) {
      TupleMap map = dict[ind];
      IntArrayList todo = target[ind];
      TupleList buf = targetTuples[ind];

      for (int cur = 0; cur < todo.size(); ++cur) {
        SrvTuple tuple = map.get(todo.getInt(cur));
        buf.add(tuple);
        // dedup
        IntArrayList key = tuple.key;
        for (int i = 0, j = 0; i < key.size(); ++i) {
          int idx = gbys[j++];
          Int2IntLinkedOpenHashMap tmp = allGroupIds[idx];
          int groupId = key.getInt(i);
          if (!tmp.containsKey(groupId)) {
            tmp.put(groupId, tmp.size());
            dependent[idx].add(groupId);
          }
          if (j == gbys.length) {
            j = 0;
          }
        }
      }
    }

    // preprocess offset
    int[] offset = new int[numAggrs.length];
    int cum = 0;
    for (int i = 0; i < numAggrs.length; ++i) {
      offset[i] = cum;
      cum += allGroupIds[i].size() * numAggrs[i];
    }

    for (int ind = 0; ind < dict.length; ++ind) {
      TupleList buf = targetTuples[ind];
      for (SrvTuple tuple : buf) {
        IntArrayList key = tuple.key;
        IntArrayList idx = tuple.getIdx();
        // fill in idx
        idx.clear();
        for (int i = 0, j = 0; i < key.size(); i++) {
          int gby = gbys[j++];
          int groupId = key.getInt(i);
          idx.add(offset[gby] + allGroupIds[gby].get(groupId) * numAggrs[gby] + cols[i]);
          if (j == gbys.length) {
            j = 0;
          }
        }
      }
    }

    return cum;
  }

  public IntArrayList parse(double[] samples) {
    condIds.clear();

    for (TupleList list : targetTuples) {
      for (SrvTuple tuple : list) {
        IntArrayList idx =  tuple.getIdx();
        double value;

        int left = 0;
        int right = tuple.range.get(0).size();
        int[] tmpBound = {left, right};
        int index;

        int keyIdx = 0;
        int rangeIdx = 0;
        int predIdx = 0;

        while (true) {
          RangeList range = tuple.range.get(rangeIdx++);

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
            condIds.add(left);
            break;
          }

          if (predIdx == preds.length) {
            predIdx = 0;
          }
        }
      }
    }

    return condIds;
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
