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

  private static <T> Map<T, Node<T>> buildGraph(Map<T, Set<T>> map) {
    Map<T, Node<T>> graph = new HashMap<T, Node<T>>();

    for (T key : map.keySet()) {
      graph.put(key, new Node<T>(key));
    }

    for (Entry<T, Set<T>> entry : map.entrySet()) {
      Node<T> child = graph.get(entry.getKey());
      for (T p : entry.getValue()) {
        Node<T> parent = graph.get(p);
        parent.addChild(child);
        child.addParent(parent);
      }
    }
    return graph;
  }

  private static <T> List<T> convert(List<Node<T>> nodes) {
    List<T> res = new ArrayList<T>();
    for (Node<T> node : nodes) {
      res.add(node.val);
    }
    return res;
  }

  private static <T> List<Node<T>> getRootNodes(Map<T, Node<T>> graph) {
    List<Node<T>> res = new ArrayList<Node<T>>();
    for (Node<T> v : graph.values()) {
      if (v.parents.isEmpty()) {
        res.add(v);
      }
    }
    return res;
  }

  private static <T> void populateNewRoots(List<Node<T>> buf1, List<Node<T>> buf2) {
    for (Node<T> node : buf1) {
      for (Node<T> child : node.children) {
        child.removeParent(node);
        if (child.parents.isEmpty()) {
          buf2.add(child);
        }
      }
    }
    buf1.clear();
  }

  public static <T> List<List<T>> getOrderByDep(Map<T, Set<T>> map) {
    Map<T, Node<T>> nodeMap = buildGraph(map);

    List<List<T>> res = new ArrayList<List<T>>();

    List<Node<T>> buf1 = getRootNodes(nodeMap);
    List<Node<T>> buf2 = new ArrayList<Node<T>>();
    List<Node<T>> tmp = null;

    while (!buf1.isEmpty()) {
      res.add(convert(buf1));
      populateNewRoots(buf1, buf2);

      tmp = buf1;
      buf1 = buf2;
      buf2 = tmp;
      // System.out.println(topNodes.size());
      // System.out.println(topNodes);
    }

    return res;
  }

  private static <T> List<Node<T>> getLeafNodes(Map<T, Node<T>> graph) {
    List<Node<T>> res = new ArrayList<Node<T>>();
    for (Node<T> v : graph.values()) {
      if (v.children.isEmpty()) {
        res.add(v);
      }
    }
    return res;
  }

  private static <T> void populateNextLevel(List<Node<T>> buf1, List<Node<T>> buf2) {
    for (Node<T> node : buf1) {
      for (Node<T> parent : node.parents) {
        buf2.add(parent);
      }
    }
    buf1.clear();
  }

  public static <T> List<List<T>> getOrderByLevel(Map<T, Set<T>> map) {
    Map<T, Node<T>> nodeMap = buildGraph(map);

    ArrayList<List<T>> tmpRes = new ArrayList<List<T>>();

    List<Node<T>> buf1 = getLeafNodes(nodeMap);
    List<Node<T>> buf2 = new ArrayList<Node<T>>();
    List<Node<T>> tmp = null;

    while (!buf1.isEmpty()) {
      tmpRes.add(convert(buf1));
      populateNextLevel(buf1, buf2);

      tmp = buf1;
      buf1 = buf2;
      buf2 = tmp;
    }

    List<List<T>> res = new ArrayList<List<T>>();
    for (int i = tmpRes.size() - 1; i >= 0; --i) {
      res.add(tmpRes.get(i));
    }

    return res;
  }

  private static Map<Integer, Set<Integer>> example1() {
    Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();

    map.put(3, new HashSet<Integer>(Arrays.asList(new Integer[] {})));
    map.put(4, new HashSet<Integer>(Arrays.asList(new Integer[] {})));
    map.put(2, new HashSet<Integer>(Arrays.asList(new Integer[] {3, 4})));
    map.put(6, new HashSet<Integer>(Arrays.asList(new Integer[] {8})));
    map.put(7, new HashSet<Integer>(Arrays.asList(new Integer[] {2, 6})));
    map.put(8, new HashSet<Integer>(Arrays.asList(new Integer[] {9})));
    map.put(9, new HashSet<Integer>(Arrays.asList(new Integer[] {})));

    return map;
  }

  private static Map<Integer, Set<Integer>> example2() {
    Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();

    map.put(1, new HashSet<Integer>(Arrays.asList(new Integer[] {})));
    map.put(2, new HashSet<Integer>(Arrays.asList(new Integer[] {})));
    map.put(0, new HashSet<Integer>(Arrays.asList(new Integer[] {1, 2})));

    return map;
  }

  public static void main(String[] args) {
    Map<Integer, Set<Integer>> map = example1();
    System.out.println(TopologicalSort.getOrderByDep(map));
    System.out.println(TopologicalSort.getOrderByLevel(map));

    System.out.println("-------------------------");

    map = example2();
    System.out.println(TopologicalSort.getOrderByDep(map));
    System.out.println(TopologicalSort.getOrderByLevel(map));
  }

}
