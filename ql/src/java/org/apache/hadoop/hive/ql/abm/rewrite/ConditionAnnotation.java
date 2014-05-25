package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.algebra.Transform;
import org.apache.hadoop.hive.ql.abm.lib.TopologicalSort;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;

public class ConditionAnnotation {

  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final HashMap<GroupByOperator, Transform[]> dependencies =
      new HashMap<GroupByOperator, Transform[]>();
  private final ArrayList<Transform> transforms = new ArrayList<Transform>();

  private final HashSet<GroupByOperator> discrete = new HashSet<GroupByOperator>();
  private final HashMap<GroupByOperator, SelectOperator> inputs =
      new HashMap<GroupByOperator, SelectOperator>();
  private final HashMap<GroupByOperator, SelectOperator> outputs =
      new HashMap<GroupByOperator, SelectOperator>();

  // <-- Used by TraceProcCtx

  public void groupByAt(GroupByOperator gby) {
    dependencies.put(gby, transforms.toArray(new Transform[transforms.size()]));
    transforms.clear();
  }

  public void conditionOn(Transform trans) {
    transforms.add(trans);
    for (AggregateInfo ai : trans.getAggregatesInvolved()) {
      GroupByOperator gby = ai.getGroupByOperator();
      TreeSet<AggregateInfo> buf = aggregates.get(gby);
      if (buf == null) {
        buf = new TreeSet<AggregateInfo>();
        aggregates.put(gby, buf);
      }
      buf.add(ai);
    }
  }

  public void combine(ConditionAnnotation other) {
    aggregates.putAll(other.aggregates);
    transforms.addAll(other.transforms);
    dependencies.putAll(other.dependencies);

    discrete.addAll(other.discrete);
  }

  // -->

  // <-- Used by RewriteProcCtx

  public void setDiscrete(GroupByOperator gby) {
    discrete.add(gby);
  }

  public void putGroupByInput(GroupByOperator gby, SelectOperator input) {
    input.getConf().cache(getInputSize(gby),
        AbmUtilities.ABM_CACHE_INPUT_PREFIX + gby.toString());
    inputs.put(gby, input);
  }

  public void putGroupByOutput(GroupByOperator gby, SelectOperator output) {
    output.getConf().cache(getOutputSize(gby),
        AbmUtilities.ABM_CACHE_OUTPUT_PREFIX + gby.toString());
    outputs.put(gby, output);
  }

  private int getInputSize(GroupByOperator gby) {
    int sz = gby.getConf().getKeys().size();
    for (AggregateInfo ai : aggregates.get(gby)) {
      if (!ai.getUdafType().equals(UdafType.COUNT)) {
        ++sz;
      }
    }
    // For tid and condition
    sz += 2;
    return sz;
  }

  private int getOutputSize(GroupByOperator gby) {
    int sz = gby.getConf().getKeys().size() + aggregates.get(gby).size();
    if (!discrete.contains(gby)) {
      // For condition, group-by-id and lineage
      sz += 3;
    } else {
      // For condition and group-by-id
      sz += 2;
    }
    return sz;
  }

  public void f() {
    // TODO:
    Map<GroupByOperator, Set<GroupByOperator>> map = getDependencyGraph();
    TopologicalSort.getOrderByLevel(map);
  }

  private Map<GroupByOperator, Set<GroupByOperator>> getDependencyGraph() {
    Map<GroupByOperator, Set<GroupByOperator>> map =
        new HashMap<GroupByOperator, Set<GroupByOperator>>();
    for (Map.Entry<GroupByOperator, Transform[]> entry : dependencies.entrySet()) {
      Set<GroupByOperator> parents = new HashSet<GroupByOperator>();
      for (Transform trans : entry.getValue()) {
        for (AggregateInfo ai : trans.getAggregatesInvolved()) {
          parents.add(ai.getGroupByOperator());
        }
      }
    }
    return map;
  }

  // -->

}
