package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.Condition;


/*
 * Class for One Dimension Interval Merge in ABM
 */
public class Merge implements IntComparator, Swapper{

  /*
   * flag indicates if the lineage of interval is incremental
   *  if false, reverse the order of intervals in output
   *
   * In simpler words, [val, Inf] is true; [-Inf, val] is false.
   *
   */
  boolean flag;
  DoubleArrayList values;
  IntArrayList ids;
  IntArrayList ind;
  List<IntArrayList> tags;

  public Merge(boolean order)
  {
    this.flag = order;
    this.values = new DoubleArrayList();
    this.ids = new IntArrayList();
    this.ind = new IntArrayList();
    this.tags = new ArrayList<IntArrayList>();
  }

  public void addPoint(double value[], int id)
  {
    if(this.flag) {
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

  public void swap(int arg0, int arg1)
  {
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
    if(this.flag)
    {
      this.values.add(Double.POSITIVE_INFINITY);
      double tmpVal = Double.POSITIVE_INFINITY;
      this.ind.add((this.values.size() - 1));
      this.tags.add(new IntArrayList());

      for(int i = this.values.size() - 2; i >= 0; i --)
      {
        if(this.values.getDouble(i) != tmpVal)
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

      for(int i = 1; i < this.values.size(); i ++)
      {
        if(this.values.getDouble(i) != tmpVal)
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
    return new Condition(id, this.values.getDouble(this.ind.getInt(start)), this.values.getDouble(this.ind.getInt(end)));
  }

  public void getLineage(IntArrayList lineage, int i)
  {
//    lineage.addAll(this.ids.subList(this.ind.getInt(i), this.ind.getInt(i + 1)));
    for(int index = this.ind.getInt(i); index < this.ind.getInt(i + 1); index ++) {
      lineage.add(this.ids.getInt(index));
    }


  }

  public IntArrayList matchLineage(IntArrayList lineage, int i)
  {
    if(this.tags.get(i).size() == 0 && lineage.size() > 0) {
      for(int index = this.ind.getInt(i); index < this.ind.getInt(i + 1); index ++)
      {
        int id = this.ids.getInt(index);
        if(lineage.contains(id))
        {
          lineage.remove(((Object)id));
          this.tags.get(i).add(id);
        }
      }
    }

    return this.tags.get(i);
  }

//  public boolean getTag(int i)
//  {
//    return this.tags.getBoolean(this.ind.getInt(i));
//  }

}

