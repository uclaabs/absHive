package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;
import org.apache.hadoop.hive.ql.abm.udaf.SorterFactory.Sorter;

public class Merge {

  private int len = -1;
  private final ArrayList<IntArrayList> dimIndexes = new ArrayList<IntArrayList>();
  private final ArrayList<IntArrayList> dimEnds = new ArrayList<IntArrayList>();
  private final ArrayList<Boolean> dimFlags = new ArrayList<Boolean>();
  private UDAFComputation op = null;

  public void addDimension(RangeList conditions) {
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
    int fstart = -1, fend = -1;

    IntArrayList indexes = dimIndexes.get(level);
    IntArrayList ends = dimEnds.get(level);

    if (!leaf) {
      for (int i = 0; i < ends.size(); ++i) {
        boolean update = false;
        boolean propagate = false;
        int start = (i == 0) ? 0 : ends.getInt(i - 1);
        int end = ends.getInt(i);
        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            // if we already find a match, then we need to push the previous matched lineage to next
            // level
            if (fstart >= 0 && !propagate) {
              // partialTerminate

              op.partialTerminate(level, indexes.getInt(fstart), (fend == indexes.size()) ? fend
                  : indexes.getInt(fend));

              enumerate(level + 1, lineage);
              for (int l = 0; l < fend; ++l) {
                if (lineage.get(indexes.getInt(l)) > level) {
                  lineage.put(indexes.getInt(l), level);
                }
              }

              propagate = true;
            }
            lineage.put(indexes.getInt(k), level);
            update = true;
          }
        }
        if (update) {
          fstart = start;
          fend = end;
        } else {
          // if no match found in this interval, there are two cases
          // 1. we haven't found any match, then do nothing
          // 2. we have found a match, then update pair (fstart, fend)
          if (fstart >= 0) {
            fend = end;
          }
        }
      }
      // check if there is unprocessed pair
      if (fstart >= 0) {
        op.partialTerminate(level, indexes.getInt(fstart), (fend == indexes.size()) ? fend
            : indexes.getInt(fend));
        enumerate(level + 1, lineage);
        for (int l = 0; l < fend; ++l) {
          if (lineage.get(indexes.getInt(l)) > level) {
            lineage.put(indexes.getInt(l), level);
          }
        }
      }
    } else {
      for (int i = 0; i < ends.size(); ++i) {
        boolean update = false;
        boolean propagate = false;
        int start = (i == 0) ? 0 : ends.getInt(i - 1);
        int end = ends.getInt(i);
        for (int k = start; k < end; ++k) {
          if (level == 0 || lineage.get(indexes.getInt(k)) == parent) {
            // if we already find a match, then we need to output the result of this match
            if (fstart >= 0 && !propagate) {
              // partial terminate
              op.partialTerminate(level, indexes.getInt(fstart), (fend == indexes.size()) ? fend
                  : indexes.getInt(fend));
              // terminate
              op.terminate();
              propagate = true;
            }
            lineage.put(indexes.getInt(k), level);
            // iterate
            op.iterate(indexes.getInt(k));
            update = true;
          }
        }
        if (update) {
          fstart = start;
          fend = end;
        } else {
          // if no match found in this interval, there are two cases
          // 1. we haven't found any match, then do nothing
          // 2. we have found a match, then update pair (fstart, fend)
          if (fstart >= 0) {
            fend = end;
          }
        }
      }

      // check if there is unprocessed pair
      if (fstart >= 0) {
        // partial terminate
        op.partialTerminate(level, indexes.getInt(fstart), (fend == indexes.size()) ? fend
            : indexes.getInt(fend));
        // terminate
        op.terminate();
      }

      op.reset();
    }
  }

  public List<Boolean> getFlags() {
    return this.dimFlags;
  }

}
