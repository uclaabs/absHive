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
import java.io.ObjectOutput;

public class BitmapObjectOutputStream implements ObjectOutput {
  byte[] buffer = null;
  int cursor = 0;

  public BitmapObjectOutputStream(int size) {
    buffer = new byte[size];
    cursor = 0;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  @Override
  public void writeInt(int arg0) throws IOException {
    buffer[cursor++] = (byte) (arg0 >> 24);
    buffer[cursor++] = (byte) (arg0 >> 16);
    buffer[cursor++] = (byte) (arg0 >> 8);
    buffer[cursor++] = (byte) (arg0);
  }

  @Override
  public void writeLong(long arg0) throws IOException {
    buffer[cursor++] = (byte) (arg0 >> 56);
    buffer[cursor++] = (byte) (arg0 >> 48);
    buffer[cursor++] = (byte) (arg0 >> 40);
    buffer[cursor++] = (byte) (arg0 >> 32);
    buffer[cursor++] = (byte) (arg0 >> 24);
    buffer[cursor++] = (byte) (arg0 >> 16);
    buffer[cursor++] = (byte) (arg0 >> 8);
    buffer[cursor++] = (byte) (arg0);
  }

  @Override
  public void write(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void write(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void writeObject(Object arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void writeByte(int arg0) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void writeBytes(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeDouble(double arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeFloat(float arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeShort(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeUTF(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChar(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChars(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeBoolean(boolean arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

}
