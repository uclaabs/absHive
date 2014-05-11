package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;

public class GroupByLineage {

  public final ArrayList<Integer> keys;
  public final ArrayList<Integer> vals;

  public GroupByLineage(ArrayList<Integer> keyCols, ArrayList<Integer> valCols) {
    keys = keyCols;
    vals = valCols;
  }

}
