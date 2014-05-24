package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;

public class SorterFactory {

  public static IntComparator getSorter(ArrayList<Condition> conditions) {
    assert !conditions.isEmpty();
    if (conditions.get(0).getFlag()) {
      return new AscendSorter(conditions, 0);
    } else {
      return new DescendSorter(conditions, 1);
    }
  }

  private static class AscendSorter implements IntComparator {

    private final ArrayList<Condition> conditions;
    private final int index;

    public AscendSorter(ArrayList<Condition> conditions, int index) {
      this.conditions = conditions;
      assert index == 0 || index == 1;
      this.index = index;
    }

    @Override
    public int compare(Integer arg0, Integer arg1) {
      return compare(arg0.intValue(), arg1.intValue());
    }

    @Override
    public int compare(int arg0, int arg1) {
      return Double.compare(
          conditions.get(arg0).getRange()[index],
          conditions.get(arg1).getRange()[index]);
    }

  }

  private static class DescendSorter extends AscendSorter {

    public DescendSorter(ArrayList<Condition> conditions, int index) {
      super(conditions, index);
    }

    @Override
    public int compare(int arg0, int arg1) {
      return -super.compare(arg0, arg1);
    }

  }

}
