package org.apache.hadoop.hive.ql.abm.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TopologicalSort<T> {

  class Node {
    T val;
    Set<Node> parents = new HashSet<Node>();
    Set<Node> children = new HashSet<Node>();
    int level = 0;

    public Node(T val) {
      this.val = val;
    }

    public void addParent(Node e) {
      parents.add(e);
    }

    public void addChild(Node e) {
      children.add(e);
    }

    public void removeParent(Node e) {
      parents.remove(e);
    }

    public T getValue() {
      return val;
    }

    @Override
    public boolean equals(Object aThat) {
      if (this == aThat) {
        return true;
      }
      if (!(aThat instanceof TopologicalSort.Node)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      Node that = (Node) aThat;
      return that.val.equals(this.val);
    }

    @Override
    public String toString() {
      return val.toString();
    }
  }

  Map<T, Node> nodeMap = new HashMap<T, Node>();

  public TopologicalSort() {
  }

  private List<Node> getTopNodes() {
    List<Node> res = new ArrayList<Node>();
    for (Node v : nodeMap.values()) {
      if (v.parents.isEmpty()) {
        res.add(v);
      }
    }
    return res;
  }

  private List<T> convert(List<Node> nodes) {
    List<T> res = new ArrayList<T>();
    for (Node node : nodes) {
      res.add(node.val);
    }
    return res;
  }

  private void removeParentsLinks(List<Node> topNodes, List<Node> buf2) {
    for (Node topNode : topNodes) {
      for (Node child : topNode.children) {
        child.removeParent(topNode);
        if (child.parents.isEmpty()) {
          buf2.add(child);
        }
      }
    }
    topNodes.clear();
  }

  public List<List<T>> getOrderByLevel(Map<T, List<T>> map) {
    for (T key : map.keySet()) {
      nodeMap.put(key, new Node(key));
    }

    for (Entry<T, List<T>> entry : map.entrySet()) {
      Node child = nodeMap.get(entry.getKey());
      for (T p : entry.getValue()) {
        Node parent = nodeMap.get(p);
        parent.addChild(child);
        child.addParent(parent);
      }
    }

    List<List<T>> res = new ArrayList<List<T>>();

    List<Node> buf1 = getTopNodes();
    List<Node> buf2 = new ArrayList<Node>();
    List<Node> tmp = null;

    while (buf1.size() > 0) {
      List<T> nodes = convert(buf1);
      removeParentsLinks(buf1, buf2);
      res.add(nodes);

      tmp = buf1;
      buf1 = buf2;
      buf2 = tmp;
      // System.out.println(topNodes.size());
      // System.out.println(topNodes);
    }

    return res;
  }

  public static void main(String[] args) {
    Map<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();

    // map.put(3, Arrays.asList(new Integer[]{}));
    // map.put(4, Arrays.asList(new Integer[]{}));
    // map.put(2, Arrays.asList(new Integer[]{3, 4}));
    // map.put(6, Arrays.asList(new Integer[]{2, 4}));
    // map.put(7, Arrays.asList(new Integer[]{2, 6}));
    // map.put(8, Arrays.asList(new Integer[]{4}));

    map.put(1, Arrays.asList(new Integer[] {}));
    map.put(2, Arrays.asList(new Integer[] {}));
    map.put(0, Arrays.asList(new Integer[] {1, 2}));

    System.out.println(new TopologicalSort<Integer>().getOrderByLevel(map));
  }

}
