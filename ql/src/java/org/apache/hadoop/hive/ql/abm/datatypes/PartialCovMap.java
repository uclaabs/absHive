package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ReferenceOpenHashMap;

import java.util.ArrayList;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class PartialCovMap {

  private final InnerCovMap[] innerGbyCovs;
  private final InterCovMap[][] interGbyCovs;
  private final int[] sizes;

  public static class InnerCovMap extends Int2ReferenceOpenHashMap<DoubleArray2D> {

    private static final long serialVersionUID = 1L;

    private final int numCovs;

    public InnerCovMap(int colSize) {
      super();
      numCovs = colSize * (colSize - 1) / 2;
    }

    public void update(int id, EWAHCompressedBitmap[] lineage1, double[] vals) {
      //
    }

  }

  public static class InterCovMap extends Long2ReferenceOpenHashMap<DoubleArray3D> {

    private static final long serialVersionUID = 1L;

    private final int numCovs;

    public InterCovMap(int lSz, int rSz) {
      super();
      numCovs = lSz * rSz;
    }

    public void update(int id1, int id2, EWAHCompressedBitmap[] lineage1,
        EWAHCompressedBitmap[] lineage2, double[] vals1, double[] vals2) {
      long id = ((long) id1 << 32) + id2;
      DoubleArray3D buf = get(id);
      if (buf == null) {
        // TODO: buf = new DoubleArray3D(...)
      }
      // TODO
    }

  }

  public PartialCovMap(int[] szs) {
    sizes = szs;

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

  public void iterate(IntArrayList gbys, IntArrayList groupIds,
      ArrayList<EWAHCompressedBitmap[]> lineages, ArrayList<double[]> values) {
    // TODO
  }

  public void merge(PartialCovMap other) {
    // TODO
  }

  public void terminate() {
    // TODO
  }

}
