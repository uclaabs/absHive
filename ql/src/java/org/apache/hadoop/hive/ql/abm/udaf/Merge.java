package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;
import org.apache.hadoop.hive.ql.abm.udaf.SorterFactory.Sorter;


public class Merge {

  private int len = -1;
  private final ArrayList<IntArrayList> dimIndexes = new ArrayList<IntArrayList>();
  private final ArrayList<IntArrayList> dimEnds = new ArrayList<IntArrayList>();
  private final ArrayList<Boolean> dimFlags = new ArrayList<Boolean>();
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
    dimFlags.add(sorter.getFlag());

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
    Int2IntOpenHashMap lineage = new Int2IntOpenHashMap();
    lineage.defaultReturnValue(-1);
    enumerate(0, lineage);
  }
  
  private void enumerate(int level, Int2IntOpenHashMap lineage) {
    boolean leaf = (level == dimIndexes.size() - 1);

    int parent = level - 1;
    int fstart = -1, fend = -1;
    
    IntArrayList indexes = dimIndexes.get(level);
    IntArrayList ends = dimEnds.get(level);
    
    System.out.println("Current Level " + level + "\t" + leaf);
    for(Integer index: indexes)
      System.out.print(index + "\t");
    System.out.println();
    for(Integer end: ends)
      System.out.print(end + "\t");
    System.out.println();
    for(Map.Entry<Integer, Integer> entry: lineage.entrySet())
      System.out.println(entry.getKey() + "\t" + entry.getValue());

    if (!leaf) {
      for (int i = 0; i < ends.size() ; ++i) {
        boolean update = false;
        boolean propagate = false;
        int start = (i == 0) ? 0 : ends.getInt(i-1) ;
        int end = ends.getInt(i);
        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            // if we already find a match, then we need to push the previous matched lineage to next level
            if(fstart >= 0 && !propagate) {
              //TODO op.partialTerminate
              System.out.println("Fstart-Fend " + fstart + "\t" + fend);
              
              op.partialTerminate(level, indexes.getInt(fstart), (fend == indexes.size()) ? fend : indexes.getInt(fend));
              
              System.out.println();
              System.out.println("before enumerate");
              for(Map.Entry<Integer, Integer> entry: lineage.entrySet())
                System.out.println(entry.getKey() + "\t" + entry.getValue());
              System.out.println();
              
              enumerate(level + 1, lineage);
              
              for (int l = 0; l < fend; ++l) {
                if (lineage.get(indexes.getInt(l)) > level) {
                  lineage.put(indexes.getInt(l), level);
                }
              }
              
              System.out.println();
              System.out.println("after enumerate");
              for(Map.Entry<Integer, Integer> entry: lineage.entrySet())
                System.out.println(entry.getKey() + "\t" + entry.getValue());
              System.out.println();
              
              propagate = true;
            }
            System.out.println("None leaf lineage match: " + indexes.getInt(k) + "\t" + level);
            lineage.put(indexes.getInt(k), level);
            update = true;
          }
        } 
        if (update) {
          fstart = start;
          fend = end;
        }
        else{
          // if no match found in this interval, there are two cases
          // 1. we haven't found any match, then do nothing
          // 2. we have found a match, then update pair (fstart, fend)
          if(fstart >= 0)
            fend = end;
        }
      }
      // check if there is unprocessed pair
      if(fstart >= 0){
        op.partialTerminate(level, indexes.getInt(fstart), (fend == indexes.size()) ? fend : indexes.getInt(fend));
        enumerate(level + 1, lineage);
        for (int l = 0; l < fend; ++l) {
          if (lineage.get(indexes.getInt(l)) > level) {
            lineage.put(indexes.getInt(l), level);
          }
        }
      }
    } else {
      for (int i = 0; i < ends.size() ; ++i) {
        
        System.out.println("Iterate " + i);
        
        boolean update = false;
        boolean propagate = false;
        int start = (i == 0) ? 0 : ends.getInt(i-1) ;
        int end = ends.getInt(i);
        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            // if we already find a match, then we need to output the result of this match
            if(fstart >= 0 && !propagate) {
              
              System.out.println("Fstart-Fend " + fstart + "\t" + fend);
              // partial terminate
              op.partialTerminate(level, indexes.getInt(fstart), (fend == indexes.size()) ? fend : indexes.getInt(fend));
              // terminate
              op.terminate();
              propagate = true;
            }
            System.out.println("lineage match: " + indexes.getInt(k) + "\t" + level);
            lineage.put(indexes.getInt(k), level);
            // TODO: call op.iterate
            op.iterate(indexes.getInt(k));
            update = true;
          }
        } 
        if (update) {
          fstart = start;
          fend = end;
        }
        else{
          // if no match found in this interval, there are two cases
          // 1. we haven't found any match, then do nothing
          // 2. we have found a match, then update pair (fstart, fend)
          if(fstart >= 0)
            fend = end;
        }
      }
      
      // check if there is unprocessed pair
      if(fstart >= 0){
        System.out.println("Fstart-Fend " + fstart + "\t" + fend);
        // partial terminate
        op.partialTerminate(level , indexes.getInt(fstart), (fend == indexes.size()) ? fend : indexes.getInt(fend));
        // terminate
        op.terminate();
      }
    }
  }
  
  public List<Boolean> getFlags()
  {
    return this.dimFlags;
  }
  
  private void oldEnumerate(int level, Int2IntOpenHashMap lineage) {
    boolean leaf = (level == dimIndexes.size() - 1);
    int parent = level - 1;
    IntArrayList indexes = dimIndexes.get(level);
    IntArrayList ends = dimEnds.get(level);

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
          op.partialTerminate(level - 1, indexes.getInt(start), (end == indexes.size()) ? end : indexes.getInt(end));
          
          System.out.println("Lineage passed to next level:");
          for(Map.Entry<Integer, Integer> entry: lineage.entrySet())
            System.out.println(entry.getKey() + "\t" + entry.getValue());
          
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
        int update = 0;
        int start = (i == 0) ? 0 : ends.getInt(i-1);
        int end = ends.getInt(i);

        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            lineage.put(indexes.getInt(k), level);
            // TODO: iterate(i)
            op.iterate(indexes.getInt(k));
            update ++;
          }
        }

        if (update > 0) {
          // partial terminate
          op.partialTerminate(level - 1, indexes.getInt(start), (end == indexes.size()) ? end : indexes.getInt(end));
          // terminate
          op.terminate();
        }
      }
    }
  }

}
