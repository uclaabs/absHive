package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;

public class KeyReader {

  private final TupleMap[] dicts;

  private final int[] numAggrs;
  private final int[][] gbysInPreds;
  private final int[][] colsInPreds;
  private final PredicateType[][] predTypes;

  private final TupleList[] tuples;
  private final IntArrayList condIds;

  public KeyReader(int[] gbyIds, int[] numAggrs,
      int[][] gbysInPreds, int[][] colsInPreds, PredicateType[][] predTypes,
      TupleMap[] srvs) {
    // Initialize basic information
    dicts = new TupleMap[gbyIds.length];
    for (int i = 0; i < gbyIds.length; ++i) {
      dicts[i] = srvs[gbyIds[i]];
    }

    this.numAggrs = numAggrs;
    // Initialize key-related fields
    this.gbysInPreds = gbysInPreds;
    this.colsInPreds = colsInPreds;
    // Initialize predicate-related fields
    this.predTypes = predTypes;

    // Initialize simulation-related fields
    tuples = new TupleList[gbyIds.length];
    for (int i = 0; i < gbyIds.length; ++i) {
      tuples[i] = new TupleList();
    }
    condIds = new IntArrayList();
  }

  public void init(IntArrayList[] targets, IntArrayList[] dependents) {
    Int2IntLinkedOpenHashMap[] uniqs = new Int2IntLinkedOpenHashMap[numAggrs.length];
    for (int i = 0; i < numAggrs.length; ++i) {
      uniqs[i] = new Int2IntLinkedOpenHashMap();
      tuples[i].clear();
      dependents[i].clear();
    }

    for (int ind = 0; ind < numAggrs.length; ++ind) {
      TupleMap map = dicts[ind];
      IntArrayList target = targets[ind];
      TupleList buf = tuples[ind];
      int[] gbys = gbysInPreds[ind];

      for (int cur = 0; cur < target.size(); ++cur) {
        SrvTuple tuple = map.get(target.getInt(cur));
        buf.add(tuple);
        // dedup
        IntArrayList key = tuple.key;
        for (int i = 0, j = 0; i < key.size(); ++i) {
          int idx = gbys[j++];
          Int2IntLinkedOpenHashMap uniq = uniqs[idx];
          int groupId = key.getInt(i);
          if (!uniq.containsKey(groupId)) {
            uniq.put(groupId, uniq.size());
            dependents[idx].add(groupId);
          }
          if (j == gbys.length) {
            j = 0;
          }
        }
      }
    }

    // preprocess offset
    int[] offsets = new int[numAggrs.length];
    int cum = 0;
    for (int i = 0; i < numAggrs.length; ++i) {
      offsets[i] = cum;
      cum += uniqs[i].size() * numAggrs[i];
    }

    for (int ind = 0; ind < numAggrs.length; ++ind) {
      TupleList buf = tuples[ind];
      int[] gbys = gbysInPreds[ind];
      int[] cols = colsInPreds[ind];
      for (SrvTuple tuple : buf) {
        IntArrayList key = tuple.key;
        IntArrayList idx = tuple.getIdx();
        // fill in idx
        idx.clear();
        for (int i = 0, j = 0; i < key.size(); i++) {
          int gby = gbys[j++];
          int groupId = key.getInt(i);
          idx.add(offsets[gby] + uniqs[gby].get(groupId) * numAggrs[gby] + cols[i]);
          if (j == gbys.length) {
            j = 0;
          }
        }
      }
    }
  }

  public IntArrayList parse(double[] samples) {
    condIds.clear();

    for (int ind = 0; ind < tuples.length; ++ind) {
      PredicateType[] preds = this.predTypes[ind];

      for (SrvTuple tuple : tuples[ind]) {
        IntArrayList idx = tuple.getIdx();
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
