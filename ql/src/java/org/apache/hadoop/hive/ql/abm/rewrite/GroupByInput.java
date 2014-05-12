package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;

public class GroupByInput {

  public final ArrayList<Integer> keys;
  public final ArrayList<Integer> vals;

  public GroupByInput(ArrayList<Integer> keyCols, ArrayList<Integer> valCols) {
    keys = keyCols;
    vals = valCols;
  }

}
