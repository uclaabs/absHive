package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.BitmapObjectOutputStream;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineageComputation extends UDAFComputation {

  private final IntListConverter converter = new IntListConverter();

  private IntArrayList currentLineage = null;

  private EWAHCompressedBitmap[] recursiveList = null;
  private final IntArrayList totalLineage = new IntArrayList();
  private final IntAVLTreeSet newLineage = new IntAVLTreeSet();

  // enumerated partial results
  private final List<List<EWAHCompressedBitmap>> bitmaps = new ArrayList<List<EWAHCompressedBitmap>>();

  // unfolded results
  private final List<EWAHCompressedBitmap> result = new ArrayList<EWAHCompressedBitmap>();
  private final List<Object> ret = new ArrayList<Object>();

  public void setGroupBitmap(IntArrayList lineage) {
    currentLineage = lineage;
    bitmaps.add(new ArrayList<EWAHCompressedBitmap>());
    totalLineage.addAll(lineage);
  }

  public void clear() {
    currentLineage = null;

    totalLineage.clear();
    newLineage.clear();
    recursiveList = null;

    bitmaps.clear();

    result.clear();
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
    while (it.hasNext()) {
      bitMap.set(it.next());
    }
    bitmaps.get(bitmaps.size() - 1).add(bitMap);
  }

  @Override
  public void unfold() {
    converter.setIntList(totalLineage);
    EWAHCompressedBitmap totalBitmap = converter.getBitmap();
    recursiveList = new EWAHCompressedBitmap[bitmaps.size() + 1];
    recursiveList[0] = totalBitmap;

    if(bitmaps.size() > 0) {
      unfoldLineageList(0);
    }
    result.add(totalBitmap);
  }

  private void unfoldLineageList(int level) {
    boolean leaf = (level == bitmaps.size() - 1);
    for (int i = 0; i < bitmaps.get(level).size(); i++) {
      recursiveList[level + 1] = bitmaps.get(level).get(i);
      if (leaf) {
        result.add(EWAHCompressedBitmap.xor(recursiveList));
      } else {
        unfoldLineageList(level + 1);
      }
    }
  }

  protected void printRes() {
    System.out.println("LinComputation Print Result");
    for(EWAHCompressedBitmap bitmap:result) {
      int[] resultArray = bitmap.toArray();
      for(int resultVal:resultArray) {
        System.out.print(resultVal + "\t");
      }
      System.out.println();
    }
  }


  @Override
  public Object serializeResult() {
//    printRes();
    BitmapObjectOutputStream oo;
    try {
      for (int i = 0; i < result.size(); ++i) {
        oo = new BitmapObjectOutputStream(result.get(i).sizeInBytes() + 12);
        result.get(i).writeExternal(oo);
        ret.add(oo.getBuffer());
      }
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

class IntListConverter implements IntComparator, Swapper {

  private IntArrayList intList = null;

  public void setIntList(IntArrayList inputList) {
    intList = inputList;
  }

  @Override
  public int compare(Integer arg0, Integer arg1) {
    return Double.compare(intList.getInt(arg0), intList.getInt(arg1));
  }

  @Override
  public int compare(int arg0, int arg1) {
    return Double.compare(intList.getInt(arg0), intList.getInt(arg1));
  }

  @Override
  public void swap(int arg0, int arg1) {
    int tmp = intList.getInt(arg0);
    intList.set(arg0, intList.getInt(arg1));
    intList.set(arg1, tmp);
  }

  public EWAHCompressedBitmap getBitmap() {
    it.unimi.dsi.fastutil.Arrays.quickSort(0, intList.size(), this, this);

    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
    for (int i = 0; i < intList.size(); i++) {
      bitmap.set(intList.getInt(i));
    }

    return bitmap;
  }
}
