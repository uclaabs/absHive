package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

public class IntArrayListSorter  implements IntComparator, Swapper{
  IntArrayList keys;

  public IntArrayListSorter(IntArrayList list) {
    this.keys = list;
  }

  public int compare(Integer o1, Integer o2) {
    return Double.compare(keys.get(o1), keys.get(o1));
  }

  public int compare(int arg0, int arg1) {
    return Double.compare(keys.get(arg0), keys.get(arg1));
  }

  @Override
  public void swap(int arg0, int arg1) {
    int tmp = keys.get(arg0);
    keys.set(arg0, keys.get(arg1));
    keys.set(arg1, tmp);
  }

  public int size() {
    return keys.size();
  }

}
