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

package org.apache.hadoop.hive.ql.abm.datatypes;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CondList {

  private final IntArrayList keyList;
  private final List<RangeList> rangeMatrix;
  private final Object[] ret = new Object[2];

  public final static ListObjectInspector intListOI = ObjectInspectorFactory
      .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  public final static ListObjectInspector doubleMatrixOI = ObjectInspectorFactory
      .getStandardListObjectInspector(
      ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
  public final static List<String> columnName = new ArrayList<String>(Arrays.asList("Keys", "Ranges"));
  public final static List<ObjectInspector> objectInspectorType =
      new ArrayList<ObjectInspector>(Arrays.asList(
          (ObjectInspector) intListOI, (ObjectInspector) doubleMatrixOI));
  public final static StructObjectInspector condListOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(columnName, objectInspectorType);

  public CondList() {
    this.keyList = new IntArrayList();
    this.rangeMatrix = new ArrayList<RangeList>();
  }

  public CondList(IntArrayList keyList, List<RangeList> rangeMatrix) {
    this.keyList = keyList;
    this.rangeMatrix = rangeMatrix;
  }

  public IntArrayList getKey() {
    return this.keyList;
  }

  public List<RangeList> getRangeMatrix() {
    return this.rangeMatrix;
  }

  public void update(int id, double value) {
    keyList.set(0, id);
    rangeMatrix.get(0).set(0, value);
  }

  public void update(int id1, int id2, double value) {
    keyList.set(0, id1);
    keyList.set(1, id2);
    rangeMatrix.get(0).set(0, value);
  }

  public void addKey(int key) {
    keyList.add(key);
  }

  public void addRangeValue(double value) {
    RangeList newlist = new RangeList();
    newlist.add(value);
    rangeMatrix.add(newlist);
  }

  public void addKeys(IntArrayList newKeys) {
    keyList.addAll(newKeys);
  }

  public void addRanges(RangeList newRanges) {
    rangeMatrix.add(newRanges);
  }

  public void addRange(double[] rangeArray) {
    for (int i = 0; i < rangeArray.length; i++) {
      this.rangeMatrix.get(i).add(rangeArray[i]);
    }
  }

  public void clear() {
    keyList.clear();
    rangeMatrix.clear();
  }

  public Object toArray() {
    ret[0] = keyList;
    ret[1] = rangeMatrix;
    return ret;
  }

}
