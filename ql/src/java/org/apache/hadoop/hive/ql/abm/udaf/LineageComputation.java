package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.LineageList;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineageComputation extends UDAFComputation {
  
  int groupCnt = -1;
  List<List<EWAHCompressedBitmap>> bitmaps = new ArrayList<List<EWAHCompressedBitmap>>();
  List<EWAHCompressedBitmap> result = new ArrayList<EWAHCompressedBitmap>();
  IntArrayList totalLineage = new IntArrayList();
  IntAVLTreeSet newLineage = new IntAVLTreeSet();
  EWAHCompressedBitmap totalBitmap = null;
  IntArrayList currentLineage = null;
  
  public void setGroupBitmap(IntArrayList lineage) {
    groupCnt ++;
    bitmaps.add(new ArrayList<EWAHCompressedBitmap>());
    currentLineage = lineage;
    totalLineage.addAll(lineage);
  }

  @Override
  public void iterate(int index) {
    newLineage.add(currentLineage.getInt(index));
  }

  @Override
  public void partialTerminate(int level, int start, int end) {
  }

  @Override
  public void terminate() {
    
    // create a new bitmap for current lineage
    EWAHCompressedBitmap bitMap = new EWAHCompressedBitmap();
    Iterator<Integer> it = this.newLineage.iterator();
    while(it.hasNext()) {
      bitMap.set(it.next());
    }
    this.bitmaps.get(groupCnt).add(bitMap);
  }

  @Override
  public void unfold() {
    
    // first convert the total lineage to a bitmap
    IntListConverter converter = new IntListConverter();
    converter.setIntList(totalLineage);
    converter.sort();
    totalBitmap = converter.getBitmap();
    
    
    unfoldLineageList(0, new EWAHCompressedBitmap());
    result.add(totalBitmap);
  }
  
  private void unfoldLineageList(int level, EWAHCompressedBitmap bitmap) {
    
    boolean leaf = (level == this.groupCnt);
    
    for(int i = 0; i < this.bitmaps.get(level).size(); i ++) {
      
      if(leaf) {
        result.add(totalBitmap.xor(bitmap.or(this.bitmaps.get(level).get(i))));
      } else {
        unfoldLineageList(level + 1, bitmap.or(this.bitmaps.get(level).get(i)));
      }  
    }
    
  }

  @Override
  public Object serializeResult() {

    try {
      return LineageList.toArray(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void reset() {
    this.newLineage.clear();
    
  }

}
