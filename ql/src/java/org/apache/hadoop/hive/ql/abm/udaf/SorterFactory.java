package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;

public class SorterFactory {

  public static Sorter getSorter(List<ConditionRange> conditions) {
    assert !conditions.isEmpty();
    boolean flag = conditions.get(0).getFlag();
    if (flag) {
      return new AscendSorter(conditions, flag);
    } else {
      return new DescendSorter(conditions, flag);
    }
  }

  public static abstract class Sorter implements IntComparator, Swapper{
    public abstract IntArrayList getIndexes();
    public abstract boolean getFlag();
  }

  private static class AscendSorter extends Sorter {

    private final IntArrayList indexes;
    private final List<ConditionRange> conditions;
    private final boolean flag;

    public AscendSorter(List<ConditionRange> conditions, boolean f) {
      this.indexes = new IntArrayList(conditions.size());

    for (int i = 0; i < conditions.size(); ++i) {
      this.indexes.add(i);
    }

      this.conditions = conditions;
      this.flag = f;
    }

    @Override
    public int compare(Integer arg0, Integer arg1) {
      return compare(arg0.intValue(), arg1.intValue());
    }

    @Override
    public int compare(int arg0, int arg1) {
      return Double.compare(
          conditions.get(this.indexes.getInt(arg0)).getValue(flag),
          conditions.get(this.indexes.getInt(arg1)).getValue(flag));
    }

    @Override
    public void swap(int arg0, int arg1) {
      int tmpId = this.indexes.get(arg0);
      this.indexes.set(arg0, this.indexes.get(arg1));
      this.indexes.set(arg1, tmpId);

    }

    @Override
    public IntArrayList getIndexes() {
      return this.indexes;
    }

    @Override
    public boolean getFlag() {
      return this.flag;
    }

  }

  private static class DescendSorter extends AscendSorter {

    public DescendSorter(List<ConditionRange> conditions, boolean f) {
      super(conditions, f);
    }

    @Override
    public int compare(int arg0, int arg1) {
      return -super.compare(arg0, arg1);
    }

  }

}
