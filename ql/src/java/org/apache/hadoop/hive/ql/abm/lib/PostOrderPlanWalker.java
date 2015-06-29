/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.abm.lib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.SemanticException;

// Note: the plan is upside-down,
// so walking the children first is actually walking the parents first.
public class PostOrderPlanWalker extends DefaultGraphWalker {

  private final HashSet<Node> visited = new HashSet<Node>();
  private final HashMap<Node, ArrayList<Node>> child2parent =
      new HashMap<Node, ArrayList<Node>>();
  private final ArrayList<Node> leaves = new ArrayList<Node>();

  public PostOrderPlanWalker(Dispatcher disp) {
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
      if (parents.size() > 1) {
        Collections.sort(parents, new TagOrderComparator());
      }
      for (Node n : parents) {
        walk(n);
      }
    }

    opStack.push(nd);
    dispatch(nd, opStack);
  }

}

class TagOrderComparator implements Comparator<Node> {

  @Override
  public int compare(Node o1, Node o2) {
    return ((ReduceSinkOperator) o1).getConf().getTag() - ((ReduceSinkOperator) o2).getConf().getTag();
  }

}