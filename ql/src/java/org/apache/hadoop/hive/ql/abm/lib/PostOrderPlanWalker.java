package org.apache.hadoop.hive.ql.abm.lib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class PreOrderWalker extends DefaultGraphWalker {

  private final HashSet<Node> visited = new HashSet<Node>();
  private final HashMap<Node, ArrayList<Node>> child2parent =
      new HashMap<Node, ArrayList<Node>>();
  private final ArrayList<Node> leaves = new ArrayList<Node>();

  public PreOrderWalker(Dispatcher disp) {
    super(disp);
  }

  private void buildGraph(Collection<? extends Node> startNodes) {
    for (Node nd : startNodes) {
      if (visited.contains(nd)) {
        continue;
      }
      visited.add(nd);

      List<? extends Node> children = nd.getChildren();

      if (children == null) {
        leaves.add(nd);
        continue;
      }

      buildGraph(children);

      for (Node child : children) {
        ArrayList<Node> parents = child2parent.get(child);
        if (parents == null) {
          parents = new ArrayList<Node>();
          child2parent.put(child, parents);
        }
        parents.add(nd);
      }
    }
  }

  @Override
  public void startWalking(Collection<Node> startNodes,
      HashMap<Node, Object> nodeOutput) throws SemanticException {
    buildGraph(startNodes);
    for (Node nd : leaves) {
      walk(nd);
    }
  }

  @Override
  public void walk(Node nd) throws SemanticException {
    ArrayList<Node> parents = child2parent.get(nd);
    if (parents != null) {
      for (Node n : parents) {
        walk(n);
      }
    }

    opStack.push(nd);
    dispatch(nd, opStack);
  }

}
