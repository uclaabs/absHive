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
      
      EWAHCompressedBitmap tmpBitmap = this.bitmaps.get(level).get(i);
      
      if(leaf) {
        result.add(totalBitmap.xor(bitmap.or(tmpBitmap)));
      } else {
        unfoldLineageList(level + 1, bitmap.or(tmpBitmap));
      }  
    }
  }

  @Override
  public Object serializeResult() {

    Object[] ret = new Object[result.size()];
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oo;
    try {
      oo = new ObjectOutputStream(baos);
      for(int i = 0; i < result.size(); i ++) {
        
        baos.reset();
        oo.reset();
        result.get(i).writeExternal(oo);
        ret[i] = ObjectInspectorUtils.copyToStandardObject(baos.toByteArray(), PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
      }
      oo.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public void reset() {
    this.newLineage.clear();
    
  }

}
