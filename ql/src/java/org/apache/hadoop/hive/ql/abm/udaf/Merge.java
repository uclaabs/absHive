package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;
import org.apache.hadoop.hive.ql.abm.udaf.SorterFactory.Sorter;


public class Merge {

  private int len = -1;
  private final ArrayList<IntArrayList> dimIndexes = new ArrayList<IntArrayList>();
  private final ArrayList<IntArrayList> dimEnds = new ArrayList<IntArrayList>();
  private UDAFComputation op = null;


  public void addDimension(List<ConditionRange> conditions) {
    if (len == -1) {
      len = conditions.size();
    } else {
      assert len == conditions.size();
    }

    Sorter sorter = SorterFactory.getSorter(conditions);

    Arrays.quickSort(0, len, sorter, sorter);
    IntArrayList indexes = sorter.getIndexes();
    dimIndexes.add(indexes);

    IntArrayList ends = new IntArrayList(len);
    for (int i = 0; i < len;) {
      int j = i + 1;
      for (; j < len && sorter.compare((j - 1), j) == 0; ++j) {
      }
      ends.add(j);
      i = j;
    }
    
    
    dimEnds.add(ends);
  }

  public void enumerate(UDAFComputation compute) {
    this.op = compute;
    enumerate(0, new Int2IntOpenHashMap());
  }

  private void enumerate(int level, Int2IntOpenHashMap lineage) {
    boolean leaf = (level == dimIndexes.size() - 1);

    int parent = level - 1;
    IntArrayList indexes = dimIndexes.get(level);
    IntArrayList ends = dimEnds.get(level);
    
    System.out.println("Current Level " + level);
    
    for(Integer index: indexes)
      System.out.print(index + "\t");
    System.out.println();
    for(Integer end: ends)
      System.out.print(end + "\t");
    System.out.println();

    if (!leaf) {
      for (int i = 0; i < ends.size() ; ++i) {
        boolean update = false;
        int start = (i == 0) ? 0 : ends.getInt(i-1) ;
        int end = ends.getInt(i);
        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            lineage.put(indexes.getInt(k), level);
            update = true;
          }
        }

        if (update) {
          // partial terminate
          op.partialTerminate(level, indexes.getInt(start), (end == indexes.size()) ? end : indexes.getInt(end));
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
        int start = (i == 0) ? 0 : ends.getInt(i-1);
        int end = ends.getInt(i);

        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            lineage.put(indexes.getInt(k), level);
            // TODO: iterate(i)
            op.iterate(indexes.getInt(k));
            update = true;
          }
        }

        if (update) {
          // partial terminate
          op.partialTerminate(level, indexes.getInt(start), (end == indexes.size()) ? end : indexes.getInt(end));
          // terminate
          op.terminate();
        }
      }
    }
  }

}
