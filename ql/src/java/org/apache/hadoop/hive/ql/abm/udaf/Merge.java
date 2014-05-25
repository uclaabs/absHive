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

//    System.out.println(len);
    
    IntArrayList ends = new IntArrayList(len);
    for (int i = 0; i < len;) {
      int j = i + 1;
      for (; j < len && sorter.compare(indexes.getInt(j - 1), indexes.getInt(j)) == 0; ++j) {
      }
      System.out.println(j + "\t" + ends.size());
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

    System.out.println("Enumerate:" + leaf + "\t" + level + "\t" + dimIndexes.size());
    
    int parent = level - 1;
    IntArrayList indexes = dimIndexes.get(level);
    IntArrayList ends = dimEnds.get(level);
    
    for(Integer t: indexes)
      System.out.print(t + "\t");
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
          // TODO: x.partialTerminate(i, j)
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
      System.out.println("Get to leaf: level" + level + "\t" + ends.size() );
      
      for (int i = 0; i < ends.size(); ++i) {
        
        
        
        boolean update = false;
        int start = (i == 0) ? 0 : ends.getInt(i-1);
        int end = ends.getInt(i);
        
        System.out.println(i + "\t" + start + "\t" + end);
        
        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            lineage.put(indexes.getInt(k), level);
            // TODO: iterate(i)
            op.iterate(indexes.getInt(k));
            update = true;
          }
        }

        if (update) {
          // TODO: partial terminate
          System.out.println("Computation Terminal: " + indexes.getInt(start) + "\t" + indexes.getInt(end));
          
          op.partialTerminate(level, indexes.getInt(start), (end == indexes.size()) ? end : indexes.getInt(end));
          // TODO: terminate
          op.terminate();
        }
      }
    }
  }


}

//class Indexes extends IntArrayList implements Swapper {
//
//  private static final long serialVersionUID = 1L;
//
//  public Indexes(int size) {
//    super(size);
//    // initialization
//    for (int i = 0; i < size; ++i) {
//      add(i);
//    }
//  }
//
//  @Override
//  public void swap(int arg0, int arg1) {
//    int tmpId = get(arg0);
//    set(arg0, get(arg1));
//    set(arg1, tmpId);
//  }
//
//}
