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

import java.io.IOException;
import java.io.ObjectInput;

public class BitmapObjectInputStream implements ObjectInput {
  byte[] buffer = null;
  int cursor = 0;

  public BitmapObjectInputStream(byte[] bytes) {
    buffer = bytes;
    cursor = 0;
  }

  @Override
  public int readInt() throws IOException {
    int value = (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    return value;
  }

  @Override
  public long readLong() throws IOException {
    long value = (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    value <<= 8;
    value ^= (buffer[cursor++] & 0xFF);
    return value;
  }

  @Override
  public boolean readBoolean() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte readByte() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public char readChar() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public double readDouble() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public float readFloat() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFully(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void readFully(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public short readShort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int skipBytes(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int available() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object readObject() throws ClassNotFoundException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long skip(long arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

}
