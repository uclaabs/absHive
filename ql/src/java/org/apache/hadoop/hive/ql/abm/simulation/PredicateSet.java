package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.abm.algebra.ComparisonTransform;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.rewrite.AggregateInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;

public class PredicateSet implements Serializable {

  private static final long serialVersionUID = 1L;

  private int[] gbys;
  private int[] cols;
  private PredicateType[] preds;

  public PredicateSet() {
  }

  public PredicateSet(ComparisonTransform[] transforms, List<GroupByOperator> ordered,
      HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates) {
    IntArrayList iGbys = new IntArrayList();
    IntArrayList iCols = new IntArrayList();
    preds = new PredicateType[transforms.length];

    int pos = 0;
    for (ComparisonTransform transform : transforms) {
      for (AggregateInfo ai : transform.getAggregatesInvolved()) {
        iGbys.add(ordered.indexOf(ai.getGroupByOperator()));
        iCols.add(getValidIndex(ai, aggregates));
      }
      preds[pos++] = getPredicateType(transform);
    }

    gbys = iGbys.toIntArray();
    cols = iCols.toIntArray();
  }

  private int getValidIndex(AggregateInfo ai,
      HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates) {
    int idx = ai.getIndex();
    if (idx == -1) {
      idx = aggregates.get(ai.getGroupByOperator()).size() - 1;
    }
    return idx;
  }

  private PredicateType getPredicateType(ComparisonTransform transform) {
    if (transform.getAggregatesInvolved().size() == 1) {
      switch (transform.getComparator()) {
      case LESS_THAN:
        return PredicateType.SINGLE_LESS_THAN;
      case LESS_THAN_EQUAL_TO:
        return PredicateType.SINGLE_LESS_THAN_OR_EQUAL_TO;
      case GREATER_THAN:
        return PredicateType.SINGLE_GREATER_THAN;
      default: // case GREATER_THAN_EQUAL_TO:
        return PredicateType.SINGLE_GREATER_THAN_OR_EQUAL_TO;
      }
    } else {
      switch (transform.getComparator()) {
      case LESS_THAN:
        return PredicateType.DOUBLE_LESS_THAN;
      case LESS_THAN_EQUAL_TO:
        return PredicateType.DOUBLE_LESS_THAN_OR_EQUAL_TO;
      case GREATER_THAN:
        return PredicateType.DOUBLE_GREATER_THAN;
      default: // case GREATER_THAN_EQUAL_TO:
        return PredicateType.DOUBLE_GREATER_THAN_OR_EQUAL_TO;
      }
    }
  }

  public int[] getGbys() {
    return gbys;
  }

  public void setGbys(int[] gbys) {
    this.gbys = gbys;
  }

  public int[] getCols() {
    return cols;
  }

  public void setCols(int[] cols) {
    this.cols = cols;
  }

  public PredicateType[] getPreds() {
    return preds;
  }

  public void setPreds(PredicateType[] preds) {
    this.preds = preds;
  }

  public void initDep(IntArrayList key, IntArrayList[] dependents, Int2IntOpenHashMap[] uniqs) {
    for (int i = 0, j = 0; i < key.size(); ++i) {
      int idx = gbys[j++];
      Int2IntOpenHashMap uniq = uniqs[idx];
      int group = key.getInt(i);
      if (!uniq.containsKey(group)) {
        uniq.put(group, uniq.size());
        dependents[idx].add(group);
      }
      if (j == gbys.length) {
        j = 0;
      }
    }
  }

  public void initIdx(IntArrayList key, Int2IntOpenHashMap[] uniqs, Offset[] offsets, int[] numAggrs, IntArrayList idx) {
    idx.clear();

    for (int i = 0, j = 0; i < key.size(); ++i) {
      int gby = gbys[j];
      idx.add(offsets[gby].offset + uniqs[gby].get(key.getInt(i)) * numAggrs[gby] + cols[j]);
      ++j;
      if (j == gbys.length) {
        j = 0;
      }
    }
  }

  public int parse(double[] samples, IntArrayList idx, List<RangeList> ranges) {
    double value;

    int left = 0;
    int right = ranges.get(0).size();
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

  public boolean isSimpleCondition() {
    return preds.length <= 1;
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
