package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineageComputation extends UDAFComputation {
  private int groupCnt = -1;
  private final List<List<EWAHCompressedBitmap>> bitmaps = new ArrayList<List<EWAHCompressedBitmap>>();
  private final List<EWAHCompressedBitmap> result = new ArrayList<EWAHCompressedBitmap>();
  private EWAHCompressedBitmap[] recursiveList = null;
  private final IntArrayList totalLineage = new IntArrayList();
  private final IntAVLTreeSet newLineage = new IntAVLTreeSet();
  private EWAHCompressedBitmap totalBitmap = null;
  private IntArrayList currentLineage = null;

  private final List<Object> ret = new ArrayList<Object>();

  public void setGroupBitmap(IntArrayList lineage) {
    groupCnt ++;
    bitmaps.add(new ArrayList<EWAHCompressedBitmap>());
    currentLineage = lineage;
    totalLineage.addAll(lineage);
  }

  public void clear() {
    bitmaps.clear();
    result.clear();
    groupCnt = -1;
    totalLineage.clear();
    newLineage.clear();
    totalBitmap = null;
    currentLineage = null;
    recursiveList = null;

    ret.clear();
  }

  @Override
  public void iterate(int index) {
    newLineage.add(currentLineage.getInt(index));
  }

  @Override
  public void partialTerminate(int level, int index) {
  }

  @Override
  public void terminate() {
    // create a new bitmap for current lineage
    EWAHCompressedBitmap bitMap = new EWAHCompressedBitmap();
    Iterator<Integer> it = newLineage.iterator();
    while(it.hasNext()) {
      bitMap.set(it.next());
    }
    bitmaps.get(groupCnt).add(bitMap);
  }

  @Override
  public void unfold() {
    IntListConverter converter = new IntListConverter();
    converter.setIntList(totalLineage);
    converter.sort();
    totalBitmap = converter.getBitmap();
    recursiveList = new EWAHCompressedBitmap[this.groupCnt + 2];
    recursiveList[0] = totalBitmap;
    unfoldLineageList(0);
    result.add(totalBitmap);
  }

  private void unfoldLineageList(int level) {
    boolean leaf = (level == groupCnt);
    for(int i = 0; i < bitmaps.get(level).size(); i ++) {
      recursiveList[level + 1] = bitmaps.get(level).get(i);
      if(leaf) {
        result.add(EWAHCompressedBitmap.xor(recursiveList));
      } else {
        unfoldLineageList(level + 1);
      }
    }
  }

  @Override
  public Object serializeResult() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oo;
    try {
      oo = new ObjectOutputStream(baos);
      for(int i = 0; i < result.size(); i ++) {
        baos.reset();
        oo.reset();
        result.get(i).writeExternal(oo);
        ret.add(ObjectInspectorUtils.copyToStandardObject(baos.toByteArray(), PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector));
      }
      oo.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public void reset() {
    newLineage.clear();
  }

}
