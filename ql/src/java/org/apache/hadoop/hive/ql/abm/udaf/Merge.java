package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.udaf.SorterFactory.Sorter;

public class Merge {

  private int len = -1;
  private final ArrayList<IntArrayList> dimIndexes = new ArrayList<IntArrayList>();
  private final List<Boolean> dimFlags;
  private UDAFComputation op = null;

  private final ArrayList<BooleanArrayList> dimDuplicates = new ArrayList<BooleanArrayList>();

  public Merge(List<Boolean> flags, List<RangeList> rangeMatrix) {
    dimFlags = flags;
    for (RangeList rangeArray : rangeMatrix) {
      addDimension(rangeArray);
    }

  //TODO
    System.out.println("Merge addDimension");
    System.out.print("booleans:");
    for(int i = 0; i < flags.size(); i ++) {
     System.out.print(flags.get(i) + ",");
    }
    System.out.println();
    System.out.print("Indexes:");
    for(IntArrayList indexes: dimIndexes) {
      for(int i = 0; i < indexes.size(); i ++) {
        System.out.print(indexes.getInt(i));
        if(i != indexes.size() - 1) {
          System.out.print(",");
        }
      }
      System.out.println();
    }

  }

  private void addDimension(RangeList conditions) {
    if (len == -1) {
      len = conditions.size();
    } else {
      assert len == conditions.size();
    }

    Sorter sorter = SorterFactory.getSorter(conditions, dimFlags.get(dimIndexes.size()));

    Arrays.quickSort(0, len, sorter, sorter);
    IntArrayList indexes = sorter.getIndexes();
    dimIndexes.add(indexes);


    BooleanArrayList duplicates = new BooleanArrayList(len);
    duplicates.add(false);
    for (int i = 1; i < len; ++i) {
      duplicates.add(sorter.compare(i-1, i) == 0);
    }
    dimDuplicates.add(duplicates);
  }

  public void enumerate(UDAFComputation compute) {
    if (dimIndexes.size() == 0) {
      return;
    }

    op = compute;
    Int2IntOpenHashMap lineage = new Int2IntOpenHashMap();
    lineage.defaultReturnValue(-1);
    enumerate(0, lineage);
  }

  private void enumerate(int level, Int2IntOpenHashMap lineage) {
    boolean leaf = (level == dimIndexes.size() - 1);
    int parent = level - 1;
    IntArrayList indexes = dimIndexes.get(level);
    BooleanArrayList duplicates = dimDuplicates.get(level);

    //TODO
    if(leaf) {
      System.out.println("Merge enumerate");
      System.out.print("Indexes:");
      for(int i = 0; i < indexes.size(); i ++) {
        System.out.print(indexes.getInt(i));
        if(i != indexes.size() - 1) {
          System.out.print(",");
        }
      }
      System.out.println();
    }


    if (!leaf) {
      // filter out the empty partition in the beginning
      int from = 0;
      for (; from < indexes.size() && lineage.get(indexes.getInt(from)) != parent; ++from) {
      }

      for (; from < indexes.size();) {
        // the starting point of a new partition
        lineage.put(indexes.getInt(from), level);
        // add all duplicates
        int to = from + 1;
        for (; to < indexes.size() && duplicates.get(to); ++to) {
          if (lineage.get(indexes.getInt(to)) == parent) {
            lineage.put(indexes.getInt(to), level);
          }
        }
        // add the empty partition following this partition
        for (; to < indexes.size() && lineage.get(indexes.getInt(to)) != parent; ++to) {
        }

        // do operation
        op.partialTerminate(level, indexes.getInt(from));
        // recursively call next level
        enumerate(level + 1, lineage);

        // recover
        for (int i = 0; i < to; ++i) {
          if (lineage.get(indexes.getInt(i)) > level) {
            lineage.put(indexes.getInt(i), level);
          }
        }

        // prepare for the next partition
        from = to;
      }
    } else {
      // filter out the empty partition in the beginning
      int from = 0;
      for (; from < indexes.size() && lineage.get(indexes.getInt(from)) != parent; ++from) {
      }

      for (; from < indexes.size();) {
        // the starting point of a new partition
        lineage.put(indexes.getInt(from), level);
        // do operation
        op.iterate(indexes.getInt(from));
        // add all duplicates
        int to = from + 1;
        for (; to < indexes.size() && duplicates.get(to); ++to) {
          if (lineage.get(indexes.getInt(to)) == parent) {
            lineage.put(indexes.getInt(to), level);
            // do operation
            op.iterate(indexes.getInt(to));
          }
        }
        // add the empty partition following this partition
        for (; to < indexes.size() && lineage.get(indexes.getInt(to)) != parent; ++to) {
        }

        // do operation
        op.partialTerminate(level, indexes.getInt(from));
        op.terminate();

        // prepare for the next partition
        from = to;
      }

      // local reset
      op.reset();
    }
  }

}
