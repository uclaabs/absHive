package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;


/*
 * Class for One Dimension Interval Merge in ABM
 */
public class Merge implements IntComparator, Swapper {

  /*
   * flag indicates if the lineage of interval is incremental
   * if false, reverse the order of intervals in output
   *
   * In simpler words, [val, Inf] is true; [-Inf, val] is false.
   */
  boolean flag;
  DoubleArrayList values;
  IntArrayList ids;
  IntArrayList ind;
  List<IntArrayList> tags;

  private int len = -1;
  private final ArrayList<Indexes> dimIndexes = new ArrayList<Indexes>();
  private final ArrayList<IntArrayList> dimEnds = new ArrayList<IntArrayList>();

  public Merge(boolean order)
  {
    this.flag = order;
    this.values = new DoubleArrayList();
    this.ids = new IntArrayList();
    this.ind = new IntArrayList();
    this.tags = new ArrayList<IntArrayList>();
  }

  public void addDimension(ArrayList<Condition> conditions) {
    if (len == -1) {
      len = conditions.size();
    } else {
      assert len == conditions.size();
    }

    IntComparator sorter = SorterFactory.getSorter(conditions);
    Indexes indexes = new Indexes(len);
    Arrays.quickSort(0, len, sorter, indexes);
    dimIndexes.add(indexes);

    IntArrayList ends = new IntArrayList(len);
    for (int i = 0; i < len;) {
      int j = i + 1;
      for (; j < len && sorter.compare(indexes.getInt(j - 1), indexes.getInt(j)) == 0; ++j) {
      }
      ends.add(j);
    }
    dimEnds.add(ends);
  }

  public void enumerate(Blahblah x) {
    this.x = x;
    enumerate(0, new Int2IntOpenHashMap());
  }

  private void enumerate(int level, Int2IntOpenHashMap lineage) {
    boolean leaf = (level == dimIndexes.size() - 1);

    int parent = level - 1;
    Indexes indexes = dimIndexes.get(level);
    IntArrayList ends = dimEnds.get(level);

    if (!leaf) {
      for (int i = 0; i < ends.size(); ++i) {
        boolean update = false;
        for (int k = (i == 0) ? 0 : ends.getInt(i-1) + 1; k < ends.getInt(i); ++k) {
          if (lineage.get(indexes.getInt(k)) == parent) {
            lineage.put(indexes.getInt(k), level);
            update = true;
          }
        }

        if (update) {
          // TODO: x.partialTerminate(i, j)
          enumerate(level + 1, lineage);

          for (int k = (i == 0) ? 0 : ends.getInt(i-1) + 1; k < ends.getInt(i); ++k) {
            if (lineage.get(indexes.getInt(k)) > level) {
              lineage.put(indexes.getInt(k), level);
            }
          }
        }
      }
    } else {
      for (int i = 0; i < ends.size(); ++i) {
        boolean update = false;
        for (int k = (i == 0) ? 0 : ends.getInt(i-1) + 1; k < ends.getInt(i); ++k) {
          if (lineage.get(indexes.getInt(k)) == parent) {
            lineage.put(indexes.getInt(k), level);
            // TODO: iterate(i)
            update = true;
          }
        }

        if (update) {
          // TODO: partial terminate
          // TODO: terminate
        }
      }
    }
  }

  public void addPoint(double value[], int id)
  {
    if (this.flag) {
      this.values.add(value[0]);
    } else {
      this.values.add(value[1]);
    }
    this.ids.add(id);
  }

  public int compare(Integer arg0, Integer arg1)
  {
    return Double.compare(values.getDouble(arg0), values.getDouble(arg1));
  }

  public int compare(int arg0, int arg1)
  {
    return Double.compare(values.getDouble(arg0), values.getDouble(arg1));
  }

  public void swap(int arg0, int arg1) {
    double tmpVal = values.getDouble(arg0);
    values.set(arg0, values.get(arg1));
    values.set(arg1, tmpVal);

    int tmpID = ids.getInt(arg0);
    ids.set(arg0, ids.getInt(arg1));
    ids.set(arg1, tmpID);
  }

  public void sort()
  {
    /*
     * Sort based on the values
     */
    Arrays.quickSort(0, values.size(), this, this);

    /*
     * Eliminate duplicate values
     */
    if (this.flag)
    {
      this.values.add(Double.POSITIVE_INFINITY);
      double tmpVal = Double.POSITIVE_INFINITY;
      this.ind.add((this.values.size() - 1));
      this.tags.add(new IntArrayList());

      for (int i = this.values.size() - 2; i >= 0; i--)
      {
        if (this.values.getDouble(i) != tmpVal)
        {
          this.ind.add(0, i);
          this.tags.add(new IntArrayList());
          tmpVal = this.values.getDouble(i);
        }
      }
    }
    else
    {
      this.values.add(0, Double.NEGATIVE_INFINITY);

      double tmpVal = Double.NEGATIVE_INFINITY;
      this.ind.add(0);
      this.tags.add(new IntArrayList());

      for (int i = 1; i < this.values.size(); i++)
      {
        if (this.values.getDouble(i) != tmpVal)
        {
          this.ind.add(i);
          this.tags.add(new IntArrayList());
          tmpVal = this.values.getDouble(i);
        }
      }
    }

  }

  public int intervalSize()
  {
    return this.ind.size() - 1;
  }

  public Condition getCondition(int id, int i)
  {
    return getCondition(id, i, i + 1);
  }

  public Condition getCondition(int id, int start, int end)
  {
    return new Condition(id, this.values.getDouble(this.ind.getInt(start)),
        this.values.getDouble(this.ind.getInt(end)));
  }

  public void getLineage(IntArrayList lineage, int i)
  {
    // lineage.addAll(this.ids.subList(this.ind.getInt(i), this.ind.getInt(i + 1)));
    int start;
    if (i == 0) {
      start = 0;
    } else {
      start = this.ind.getInt(i - 1) + 1;
    }

    for (int index = start; index <= this.ind.getInt(i); index++) {
      lineage.add(this.ids.getInt(index));
    }
  }

  public IntArrayList matchLineage(IntArrayList lineage, int i)
  {
    if (this.tags.get(i).size() == 0 && lineage.size() > 0) {

      int start;
      if (i == 0) {
        start = 0;
      } else {
        start = this.ind.getInt(i - 1) + 1;
      }

      for (int index = start; index <= this.ind.getInt(i); index++)
      {
        int id = this.ids.getInt(index);
        if (lineage.contains(id))
        {
          lineage.remove(((Object) id));
          this.tags.get(i).add(id);
        }
      }
    }

    return this.tags.get(i);
  }

  public void print()
  {
    for (int i = 0; i < this.ids.size(); i++) {
      System.out.print(this.ids.getInt(i) + "\t");
    }
    System.out.println();

    for (int i = 0; i < this.values.size(); i++) {
      System.out.print(this.values.getDouble(i) + "\t");
    }
    System.out.println();

    for (int i = 0; i < this.ind.size(); i++) {
      System.out.print(this.ind.getInt(i) + "\t");
    }
    System.out.println();
  }

  // public boolean getTag(int i)
  // {
  // return this.tags.getBoolean(this.ind.getInt(i));
  // }

}

class Indexes extends IntArrayList implements Swapper {

  private static final long serialVersionUID = 1L;

  public Indexes(int size) {
    super(size);
    // initialization
    for (int i = 0; i < size; ++i) {
      add(i);
    }
  }

  @Override
  public void swap(int arg0, int arg1) {
    int tmpId = get(arg0);
    set(arg0, get(arg1));
    set(arg1, tmpId);
  }

}
