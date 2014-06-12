package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.algebra.ComparisonTransform;
import org.apache.hadoop.hive.ql.abm.datatypes.Conditions;
import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;

public class KeyReader implements Serializable {

  private static final long serialVersionUID = 1L;

  private int[] uniqGbys;
  private int[] gbys;
  private int[] cols;
  private PredicateType[] preds;

  private transient List<RangeList> ranges;
  private transient IntArrayList idx = new IntArrayList();

  public KeyReader() {
  }

  public KeyReader(ComparisonTransform[] predicates) {
    // TODO: initialize uniqGbys & gbys & cols & preds
  }

  public void init(Conditions condition, IntArrayList[] groups, int[] numAggrs) {
    ranges = condition.getRanges();
    // intopenhashset
    // TODO: initialize idx & fill in groups
  }

  public int parse(double[] samples) {
    int left = 0;
    int right = ranges.get(0).size();

    int cur = 0;
    int pos = 0;
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