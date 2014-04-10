/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.rcfile.merge;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.merge.MergeMapper;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class RCFileMergeMapper extends MergeMapper implements
    Mapper<Object, RCFileValueBufferWrapper, Object, Object> {

  protected CompressionCodec codec = null;
  protected int columnNumber = 0;

  private RCFile.Writer outWriter;

  @Override
  public void map(Object k, RCFileValueBufferWrapper value,
      OutputCollector<Object, Object> output, Reporter reporter)
      throws IOException {
    try {

      RCFileKeyBufferWrapper key = null;
      if (k instanceof CombineHiveKey) {
        key = (RCFileKeyBufferWrapper) ((CombineHiveKey) k).getKey();
      } else {
        key = (RCFileKeyBufferWrapper) k;
      }

      checkAndFixTmpPath(key.inputPath);

      if (outWriter == null) {
        codec = key.codec;
        columnNumber = key.keyBuffer.getColumnNumber();
        jc.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNumber);
        outWriter = new RCFile.Writer(fs, jc, outPath, null, codec);
      }

      boolean sameCodec = ((codec == key.codec) || codec.getClass().equals(
          key.codec.getClass()));

      if ((key.keyBuffer.getColumnNumber() != columnNumber) || (!sameCodec)) {
        throw new IOException(
            "RCFileMerge failed because the input files use different CompressionCodec or have different column number setting.");
      }

      outWriter.flushBlock(key.keyBuffer, value.valueBuffer, key.recordLength,
          key.keyLength, key.compressedKeyLength);
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // close writer
    if (outWriter == null) {
      return;
    }

    outWriter.close();
    outWriter = null;

    if (!exception) {
      FileStatus fss = fs.getFileStatus(outPath);
      LOG.info("renamed path " + outPath + " to " + finalPath
          + " . File size is " + fss.getLen());
      if (!fs.rename(outPath, finalPath)) {
        throw new IOException("Unable to rename output to " + finalPath);
      }
    } else {
      if (!autoDelete) {
        fs.delete(outPath, true);
      }
    }
  }

}
