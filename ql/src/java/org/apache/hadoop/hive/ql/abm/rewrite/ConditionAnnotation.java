package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;

public class ConditionAnnotation implements Comparator<GroupByOperator> {

  private final HashMap<GroupByOperator, Integer> positions =
      new HashMap<GroupByOperator, Integer>();
  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final TreeSet<GroupByOperator> topLevel = new TreeSet<GroupByOperator>(this);
  private final HashMap<GroupByOperator, GroupByOperator[]> dependencies =
      new HashMap<GroupByOperator, GroupByOperator[]>();

  public void groupByAt(GroupByOperator gby) {
    if (!topLevel.isEmpty()) {
      dependencies.put(gby, topLevel.toArray(new GroupByOperator[topLevel.size()]));
      topLevel.clear();
    }
  }

  public void conditionOn(AggregateInfo aggr) {
    GroupByOperator gby =  aggr.getGroupByOperator();
    TreeSet<AggregateInfo> buf = aggregates.get(gby);
    if (buf == null) {
      buf = new TreeSet<AggregateInfo>();
      aggregates.put(gby, buf);

      positions.put(gby, positions.size());
      topLevel.add(gby);
    }
    buf.add(aggr);
  }

  @SuppressWarnings("unchecked")
  public void combine(ConditionAnnotation other) {
    for (GroupByOperator gby : other.positions.keySet()) {
      assert !positions.containsKey(gby);
      positions.put(gby, positions.size());
    }

    for (Map.Entry<GroupByOperator, TreeSet<AggregateInfo>> entry : other.aggregates.entrySet()) {
      aggregates.put(entry.getKey(), (TreeSet<AggregateInfo>) entry.getValue().clone());
    }

    topLevel.addAll(other.topLevel);

    for (Map.Entry<GroupByOperator, GroupByOperator[]> entry : other.dependencies.entrySet()) {
      dependencies.put(entry.getKey(), entry.getValue().clone());
    }
  }

  public void check() {
    for (Map.Entry<GroupByOperator, TreeSet<AggregateInfo>> entry : aggregates.entrySet()) {
      GroupByOperator gby = entry.getKey();
      TreeSet<AggregateInfo> aggrs = entry.getValue();
      assert gby.getConf().getAggregators().size() + 1 == aggrs.size();
    }
  }

  @Override
  public int compare(GroupByOperator o1, GroupByOperator o2) {
    return positions.get(o1) - positions.get(o2);
  }

}
