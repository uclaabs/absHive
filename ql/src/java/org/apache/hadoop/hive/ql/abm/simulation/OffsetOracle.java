package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class OffsetOracle extends Long2IntOpenHashMap {

  private static final long serialVersionUID = 1L;

  private int cumOffset;

  public int put(int gby, int group, int offset) {
    long key = ((long) gby << 32) + group;
    return put(key, offset);
  }

  public int get(int gby, int group) {
    long key = ((long) gby << 32) + group;
    return get(key);
  }

  public int getCumOffset() {
    return cumOffset;
  }

  public void setCumOffset(int cumOffset) {
    this.cumOffset = cumOffset;
  }

}
