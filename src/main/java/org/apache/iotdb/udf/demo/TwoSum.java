/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.udf.demo;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

public class TwoSum implements UDTF {


  private Type dataType;


  @Override
  public void transform(Column[] columns, ColumnBuilder builder) {
    int count;
    switch (dataType) {
      case INT32:
        count = columns[0].getPositionCount();

        if (columns[0].mayHaveNull() || columns[1].mayHaveNull()) {
          for (int i = 0; i < count; i++) {
            if (columns[0].isNull(i) || columns[1].isNull(0)) {
              builder.appendNull();
            } else {
              builder.writeLong(((long) columns[0].getInt(i)) + columns[1].getInt(i));
            }
          }
        } else {
          int[] inputs1 = columns[0].getInts();
          int[] inputs2 = columns[1].getInts();
          for (int i = 0; i < count; i++) {
            builder.writeLong(((long) inputs1[i]) + inputs2[i]);
          }
        }
        break;
      case INT64:
        count = columns[0].getPositionCount();

        if (columns[0].mayHaveNull() || columns[1].mayHaveNull()) {
          for (int i = 0; i < count; i++) {
            if (columns[0].isNull(i) || columns[1].isNull(0)) {
              builder.appendNull();
            } else {
              builder.writeDouble(((double) columns[0].getLong(i)) + columns[1].getLong(i));
            }
          }
        } else {
          long[] inputs1 = columns[0].getLongs();
          long[] inputs2 = columns[1].getLongs();
          for (int i = 0; i < count; i++) {
            builder.writeDouble(((double) inputs1[i]) + inputs2[i]);
          }
        }
        break;
      case FLOAT:
        count = columns[0].getPositionCount();

        if (columns[0].mayHaveNull() || columns[1].mayHaveNull()) {
          for (int i = 0; i < count; i++) {
            if (columns[0].isNull(i) || columns[1].isNull(0)) {
              builder.appendNull();
            } else {
              builder.writeDouble(((double) columns[0].getFloat(i)) + columns[1].getFloat(i));
            }
          }
        } else {
          float[] inputs1 = columns[0].getFloats();
          float[] inputs2 = columns[1].getFloats();
          for (int i = 0; i < count; i++) {
            builder.writeDouble(((double) inputs1[i]) + inputs2[i]);
          }
        }
        break;
      case DOUBLE:
        count = columns[0].getPositionCount();

        if (columns[0].mayHaveNull() || columns[1].mayHaveNull()) {
          for (int i = 0; i < count; i++) {
            if (columns[0].isNull(i) || columns[1].isNull(0)) {
              builder.appendNull();
            } else {
              builder.writeDouble((columns[0].getDouble(i)) + columns[1].getDouble(i));
            }
          }
        } else {
          double[] inputs1 = columns[0].getDoubles();
          double[] inputs2 = columns[1].getDoubles();
          for (int i = 0; i < count; i++) {
            builder.writeDouble(inputs1[i] + inputs2[i]);
          }
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public void validate(UDFParameterValidator validator) {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validateInputSeriesDataType(1, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    dataType = parameters.getDataType(0);
    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(dataType == Type.INT32 ? Type.INT64 : Type.DOUBLE);
  }
}
