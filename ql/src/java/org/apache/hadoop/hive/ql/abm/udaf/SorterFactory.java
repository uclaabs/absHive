package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;

public class SorterFactory {

  public static Sorter getSorter(RangeList conditions, boolean flag) {
    assert !conditions.isEmpty();
    if (flag) {
      return new AscendSorter(conditions);
    } else {
      return new DescendSorter(conditions);
    }
  }

  public static abstract class Sorter implements IntComparator, Swapper {
    public abstract IntArrayList getIndexes();
  }

  private static class AscendSorter extends Sorter {

    private final IntArrayList indexes;
    private final RangeList conditions;

    public AscendSorter(RangeList conditions) {
      indexes = new IntArrayList(conditions.size());

      for (int i = 0; i < conditions.size(); ++i) {
        indexes.add(i);
      }

      this.conditions = conditions;
    }

    @Override
    public int compare(Integer arg0, Integer arg1) {
      return compare(arg0.intValue(), arg1.intValue());
    }

    @Override
    public int compare(int arg0, int arg1) {
      return Double.compare(
          conditions.get(indexes.getInt(arg0)),
          conditions.get(indexes.getInt(arg1)));
    }

    @Override
    public void swap(int arg0, int arg1) {
      int tmpId = indexes.get(arg0);
      indexes.set(arg0, indexes.get(arg1));
      indexes.set(arg1, tmpId);

    }

    @Override
    public IntArrayList getIndexes() {
      return indexes;
    }

  }

  private static class DescendSorter extends AscendSorter {

    public DescendSorter(RangeList conditions) {
      super(conditions);
    }

    @Override
    public int compare(int arg0, int arg1) {
      return -super.compare(arg0, arg1);
    }

  }

}
