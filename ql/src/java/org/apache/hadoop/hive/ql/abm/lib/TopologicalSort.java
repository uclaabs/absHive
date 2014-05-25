package org.apache.hadoop.hive.ql.abm.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TopologicalSort {

  private static class Node<T> {
    T val;
    Set<Node<T>> parents = new HashSet<Node<T>>();
    Set<Node<T>> children = new HashSet<Node<T>>();

    public Node(T val) {
      this.val = val;
    }

    public void addParent(Node<T> e) {
      parents.add(e);
    }

    public void addChild(Node<T> e) {
      children.add(e);
    }

    public void removeParent(Node<T> e) {
      parents.remove(e);
    }

    @Override
    public boolean equals(Object aThat) {
      if (this == aThat) {
        return true;
      }
      if (!(aThat instanceof TopologicalSort.Node)) {
        return false;
      }
      Node<?> that = (Node<?>) aThat;
      return that.val.equals(this.val);
    }

    @Override
    public String toString() {
      return val.toString();
    }
  }

  private static <T> List<Node<T>> getTopNodes(Map<T, Node<T>> nodeMap) {
    List<Node<T>> res = new ArrayList<Node<T>>();
    for (Node<T> v : nodeMap.values()) {
      if (v.parents.isEmpty()) {
        res.add(v);
      }
    }
    return res;
  }

  private static <T> List<T> convert(List<Node<T>> nodes) {
    List<T> res = new ArrayList<T>();
    for (Node<T> node : nodes) {
      res.add(node.val);
    }
    return res;
  }

  private static <T> void removeParentsLinks(List<Node<T>> topNodes, List<Node<T>> buf2) {
    for (Node<T> topNode : topNodes) {
      for (Node<T> child : topNode.children) {
        child.removeParent(topNode);
        if (child.parents.isEmpty()) {
          buf2.add(child);
        }
      }
    }
    topNodes.clear();
  }

  public static <T> List<List<T>> getOrderByLevel(Map<T, List<T>> map) {
    Map<T, Node<T>> nodeMap = new HashMap<T, Node<T>>();

    for (T key : map.keySet()) {
      nodeMap.put(key, new Node<T>(key));
    }

    for (Entry<T, List<T>> entry : map.entrySet()) {
      Node<T> child = nodeMap.get(entry.getKey());
      for (T p : entry.getValue()) {
        Node<T> parent = nodeMap.get(p);
        parent.addChild(child);
        child.addParent(parent);
      }
    }

    List<List<T>> res = new ArrayList<List<T>>();

    List<Node<T>> buf1 = getTopNodes(nodeMap);
    List<Node<T>> buf2 = new ArrayList<Node<T>>();
    List<Node<T>> tmp = null;

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
    // map.put(6, Arrays.asList(new Integer[]{4}));
    // map.put(7, Arrays.asList(new Integer[]{2, 6}));
    // map.put(8, Arrays.asList(new Integer[]{4}));

    map.put(1, Arrays.asList(new Integer[] {}));
    map.put(2, Arrays.asList(new Integer[] {}));
    map.put(0, Arrays.asList(new Integer[] {1, 2}));

    System.out.println(TopologicalSort.getOrderByLevel(map));
  }

}
