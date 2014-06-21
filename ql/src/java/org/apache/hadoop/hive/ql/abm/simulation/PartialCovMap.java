package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ReferenceMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceOpenHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray2D;
import org.apache.hadoop.hive.ql.abm.datatypes.DoubleArray3D;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class PartialCovMap implements Serializable {

  private static final long serialVersionUID = 1L;

  private final InnerCovMap[] innerGbyCovs;
  private final InterCovMap[][] interGbyCovs;

  public static class InnerCovMap extends Int2ReferenceOpenHashMap<DoubleArray2D> {

    private static final long serialVersionUID = 1L;

    private final int numCols;
    private final int numCovs;

    public InnerCovMap(int colSize) {
      super();
      numCols = colSize;
      numCovs = colSize * (colSize - 1) / 2;
    }

    public void update(int tid, int groupId, EWAHCompressedBitmap[] lineage, double[] vals) {
      DoubleArray2D buf = get(groupId);

      int numRows = lineage.length; // number of conditions

      if (buf == null) {
        buf = new DoubleArray2D(numRows, numCovs, numCols);
        put(groupId, buf);
      }

      --numRows;

      EWAHCompressedBitmap baseLineage = lineage[numRows];
      if (baseLineage.get(tid)) {
        // check L(1) .. L(n-1)
        for (int i = 0; i < numRows; ++i) {
          if (!lineage[i].get(tid)) {
            // update Sum(XY) in this condition
            buf.updateRow(i, vals);
          }
        }
      } else {
        // it is for base
        // add it to the last
        buf.updateRow(numRows, vals);
      }
    }
  }

  public static class InterCovMap extends Long2ReferenceOpenHashMap<DoubleArray3D> {

    private static final long serialVersionUID = 1L;

    private final int numCovs;
    private final int rows1;
    private final int rows2;

    public InterCovMap(int size1, int size2) {
      super();
      numCovs = size1 * size2;
      rows1 = size1;
      rows2 = size2;
    }

    public DoubleArray3D get(int groupId1, int groupId2) {
      return super.get(((long) groupId1 << 32) + groupId2);
    }

    public void update(int tid, int groupId1, int groupId2, EWAHCompressedBitmap[] lineage1,
        EWAHCompressedBitmap[] lineage2, double[] vals1, double[] vals2) {
      long id = ((long) groupId1 << 32) + groupId2;
      DoubleArray3D buf = get(id);

      int numRow1 = lineage1.length;
      int numRow2 = lineage2.length;

      if (buf == null) {
        buf = new DoubleArray3D(numRow1, numRow2, numCovs, rows1, rows2);
        put(id, buf);
      }

      --numRow1;
      --numRow2;

      EWAHCompressedBitmap baseLineage1 = lineage1[numRow1];
      EWAHCompressedBitmap baseLineage2 = lineage2[numRow2];

      boolean flag1 = baseLineage1.get(tid);
      boolean flag2 = baseLineage2.get(tid);

      if (flag1 & flag2) {
        // both of them contain this tuple
        for (int i = 0; i < numRow1; ++i) {
          if (!lineage1[i].get(tid)) {
            for (int j = 0; j < numRow2; ++j) {
              if (!lineage2[j].get(tid)) {
                buf.updateRow(i, j, vals1, vals2);
              }
            }
          }
        }
        // end of if
      } else if (flag1 && !flag2) {
        for (int i = 0; i < numRow1; ++i) {
          if (!lineage1[i].get(tid)) {
            buf.updateRow(i, numRow2, vals1, vals2);
          }
        }
      } else if (!flag1 && flag2) {
        for (int i = 0; i < numRow2; ++i) {
          if (!lineage2[i].get(tid)) {
            buf.updateRow(numRow1, i, vals1, vals2);
          }
        }
      } else {
        // both of them are base
        buf.updateRow(numRow1, numRow2, vals1, vals2);
      }
    }
  }

  public PartialCovMap(int[] sizes) {
    innerGbyCovs = new InnerCovMap[sizes.length];
    for (int i = 0; i < sizes.length; ++i) {
      innerGbyCovs[i] = new InnerCovMap(sizes[i]);
    }

    interGbyCovs = new InterCovMap[sizes.length][];
    for (int i = 0; i < sizes.length; ++i) {
      interGbyCovs[i] = new InterCovMap[sizes.length];
      for (int j = i + 1; j < sizes.length; ++j) {
        interGbyCovs[i][j] = new InterCovMap(sizes[i], sizes[j]);
      }
    }
  }

  public InnerCovMap[] getInnerGbyCovs() {
    return innerGbyCovs;
  }

  public InterCovMap[][] getInterGbyCovs() {
    return interGbyCovs;
  }

  public void iterate(int tid, IntArrayList gbys, IntArrayList groupIds,
      ArrayList<EWAHCompressedBitmap[]> lineages, ArrayList<double[]> values) {
    int length = gbys.size();
    // then update every InnerCovMap
    for (int i = 0; i < length; ++i) {

      int groupOpId1 = gbys.getInt(i);
      InnerCovMap innerCovMap = innerGbyCovs[groupOpId1];
      innerCovMap.update(tid, groupIds.getInt(i), lineages.get(i), values.get(i));

      for (int j = i + 1; j < length; ++j) {
        int groupOpId2 = gbys.getInt(j);
        InterCovMap interCovMap = interGbyCovs[groupOpId1][groupOpId2];
        interCovMap.update(tid, groupIds.getInt(i), groupIds.getInt(j), lineages.get(i),
            lineages.get(j), values.get(i), values.get(j));
      }
    }
  }

  public void merge(PartialCovMap partialMap) {
    // update innerGbyCovs
    for (int i = 0; i < innerGbyCovs.length; ++i) {
      InnerCovMap currentMap = innerGbyCovs[i];
      for (Int2ReferenceMap.Entry<DoubleArray2D> entry : partialMap.innerGbyCovs[i]
          .int2ReferenceEntrySet()) {
        int groupOpId = entry.getIntKey();
        DoubleArray2D inputArray = entry.getValue();
        DoubleArray2D currentArray = currentMap.get(groupOpId);

        if (currentArray == null) {
          currentMap.put(groupOpId, inputArray);
        } else {
          currentArray.merge(inputArray);
        }
      }
    }

    // update interGbyCovs
    for (int i = 0; i < interGbyCovs.length; ++i) {
      InterCovMap[] interGbyCovList = interGbyCovs[i];
      for (int j = i + 1; j < interGbyCovList.length; ++j) {
        InterCovMap currentMap = interGbyCovs[i][j];

        for (Long2ReferenceMap.Entry<DoubleArray3D> entry : partialMap.interGbyCovs[i][j]
            .long2ReferenceEntrySet()) {
          long id = entry.getLongKey();
          DoubleArray3D inputArray = entry.getValue();
          DoubleArray3D currentArray = currentMap.get(id);

          if (currentArray == null) {
            currentMap.put(id, inputArray);
          } else {
            currentArray.merge(inputArray);
          }
        }
      }
    }
  }

  public void terminate() {
    for (int i = 0; i < innerGbyCovs.length; ++i) {
      InnerCovMap currentMap = innerGbyCovs[i];
      for (DoubleArray2D currentArray : currentMap.values()) {
        currentArray.updateByBase();
      }
    }

    for (int i = 0; i < interGbyCovs.length; ++i) {
      InterCovMap[] interGbyCovList = interGbyCovs[i];

      for (int j = i + 1; j < interGbyCovList.length; ++j) {
        InterCovMap currentMap = interGbyCovs[i][j];
        for (DoubleArray3D inputArray : currentMap.values()) {
          inputArray.updateByBase();
        }
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    boolean first = true;
    for (int i = 0; i < innerGbyCovs.length; ++i) {
      if (!first) {
        builder.append('\n');
      }
      first = false;

      InnerCovMap currentMap = innerGbyCovs[i];
      builder.append("<" + i + ">: ");
      for (Map.Entry<Integer, DoubleArray2D> entry : currentMap.entrySet()) {
        DoubleArray2D currentArray = entry.getValue();
        builder.append("(" + entry.getKey() + ") = " + currentArray.toString());
      }
    }

    first = true;
    for (int i = 0; i < interGbyCovs.length; ++i) {
      InterCovMap[] interGbyCovList = interGbyCovs[i];
      for (int j = i + 1; j < interGbyCovList.length; ++j) {
        if (!first) {
          builder.append('\n');
        }
        first = false;

        InterCovMap currentMap = interGbyCovs[i][j];
        builder.append("<" + i + ", " + j + ">: ");
        for (Map.Entry<Long, DoubleArray3D> entry : currentMap.entrySet()) {
          DoubleArray3D currentArray = entry.getValue();
          builder.append("(" + entry.getKey() + ") = " + currentArray.toString());
        }
      }
    }

    return builder.toString();
  }
}