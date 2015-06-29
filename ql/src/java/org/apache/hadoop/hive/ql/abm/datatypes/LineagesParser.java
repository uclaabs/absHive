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

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineagesParser {

  private final ListObjectInspector oi;
  private final EWAHCompressedBitmapParser parser;

  public LineagesParser(ObjectInspector oi) {
    this.oi = (ListObjectInspector) oi;
    parser = new EWAHCompressedBitmapParser(this.oi.getListElementObjectInspector());
  }

  public EWAHCompressedBitmap[] parse(Object o) {
    int length = oi.getListLength(o);
    EWAHCompressedBitmap[] bitmaps = new EWAHCompressedBitmap[length];
    for (int i = 0; i < length; ++i) {
      bitmaps[i] = parser.parse(oi.getListElement(o, i));
    }
    return bitmaps;
  }

}
