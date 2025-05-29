/*! ******************************************************************************
 *
 * CPython for the Hop orchestration platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.phalanxdev.python;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.util.Text;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Apache Arrow utilities for fast data transfer between Java and Python
 */
public class ArrowUtils {

  // Shared buffer allocator
  private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /**
   * Convert Hop rows to Arrow format
   */
  public static byte[] rowsToArrow(IRowMeta meta, List<Object[]> rows) throws HopException {
    // Create Arrow schema from Hop metadata
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < meta.size(); i++) {
      IValueMeta valueMeta = meta.getValueMeta(i);
      Field field = createArrowField(valueMeta);
      fields.add(field);
    }
    Schema schema = new Schema(fields);

    // Create vectors and populate with data
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();
      
      // Populate data
      for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
        Object[] row = rows.get(rowIdx);
        for (int colIdx = 0; colIdx < meta.size(); colIdx++) {
          IValueMeta valueMeta = meta.getValueMeta(colIdx);
          FieldVector vector = root.getVector(colIdx);
          setVectorValue(vector, rowIdx, row[colIdx], valueMeta);
        }
      }
      root.setRowCount(rows.size());

      // Write to Arrow IPC stream format
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (WritableByteChannel channel = Channels.newChannel(out);
           ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      
      return out.toByteArray();
    } catch (IOException e) {
      throw new HopException("Error converting rows to Arrow format", e);
    }
  }

  /**
   * Convert Arrow data to Hop rows
   */
  public static PythonSession.RowMetaAndRows arrowToRows(byte[] arrowData) throws HopException {
    PythonSession.RowMetaAndRows result = new PythonSession.RowMetaAndRows();
    List<Object[]> rowsList = new ArrayList<>();
    
    try (ByteArrayInputStream in = new ByteArrayInputStream(arrowData);
         ReadableByteChannel channel = Channels.newChannel(in);
         ArrowStreamReader reader = new ArrowStreamReader(channel, allocator)) {
      
      // Read schema and create Hop metadata
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      result.m_rowMeta = createHopRowMeta(schema);
      
      // Read all batches
      while (reader.loadNextBatch()) {
        int rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; i++) {
          Object[] row = new Object[schema.getFields().size()];
          for (int j = 0; j < schema.getFields().size(); j++) {
            FieldVector vector = root.getVector(j);
            row[j] = getVectorValue(vector, i, result.m_rowMeta.getValueMeta(j));
          }
          rowsList.add(row);
        }
      }
      
      // Convert list to array
      result.m_rows = rowsList.toArray(new Object[rowsList.size()][]);
      
      return result;
    } catch (Exception e) {
      throw new HopException("Error converting Arrow data to rows", e);
    }
  }

  /**
   * Create Arrow field from Hop value metadata
   */
  private static Field createArrowField(IValueMeta valueMeta) {
    String name = valueMeta.getName();
    ArrowType arrowType;
    
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        arrowType = new ArrowType.Utf8();
        break;
      case IValueMeta.TYPE_INTEGER:
        arrowType = new ArrowType.Int(64, true); // 64-bit signed integer
        break;
      case IValueMeta.TYPE_NUMBER:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        arrowType = new ArrowType.Decimal(38, 10, 128); // 128-bit decimal
        break;
      case IValueMeta.TYPE_DATE:
        arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
        break;
      case IValueMeta.TYPE_TIMESTAMP:
        arrowType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        break;
      case IValueMeta.TYPE_BOOLEAN:
        arrowType = new ArrowType.Bool();
        break;
      case IValueMeta.TYPE_BINARY:
        arrowType = new ArrowType.Binary();
        break;
      default:
        arrowType = new ArrowType.Utf8(); // Default to string
    }
    
    FieldType fieldType = new FieldType(true, arrowType, null);
    return new Field(name, fieldType, null);
  }

  /**
   * Create Hop row metadata from Arrow schema
   */
  private static IRowMeta createHopRowMeta(Schema schema) throws HopException {
    IRowMeta rowMeta = new RowMeta();
    
    for (Field field : schema.getFields()) {
      String name = field.getName();
      int hopType = IValueMeta.TYPE_STRING; // Default
      
      ArrowType arrowType = field.getType();
      if (arrowType instanceof ArrowType.Utf8) {
        hopType = IValueMeta.TYPE_STRING;
      } else if (arrowType instanceof ArrowType.Int) {
        hopType = IValueMeta.TYPE_INTEGER;
      } else if (arrowType instanceof ArrowType.FloatingPoint) {
        hopType = IValueMeta.TYPE_NUMBER;
      } else if (arrowType instanceof ArrowType.Decimal) {
        hopType = IValueMeta.TYPE_BIGNUMBER;
      } else if (arrowType instanceof ArrowType.Date) {
        hopType = IValueMeta.TYPE_DATE;
      } else if (arrowType instanceof ArrowType.Timestamp) {
        hopType = IValueMeta.TYPE_TIMESTAMP;
      } else if (arrowType instanceof ArrowType.Bool) {
        hopType = IValueMeta.TYPE_BOOLEAN;
      } else if (arrowType instanceof ArrowType.Binary) {
        hopType = IValueMeta.TYPE_BINARY;
      }
      
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, hopType);
      rowMeta.addValueMeta(valueMeta);
    }
    
    return rowMeta;
  }

  /**
   * Set value in Arrow vector
   */
  private static void setVectorValue(FieldVector vector, int index, Object value, IValueMeta valueMeta) 
      throws HopValueException {
    if (value == null) {
      vector.setNull(index);
      return;
    }
    
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        ((VarCharVector) vector).setSafe(index, new Text(valueMeta.getString(value)));
        break;
      case IValueMeta.TYPE_INTEGER:
        ((BigIntVector) vector).setSafe(index, valueMeta.getInteger(value));
        break;
      case IValueMeta.TYPE_NUMBER:
        ((Float8Vector) vector).setSafe(index, valueMeta.getNumber(value));
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        BigDecimal bd = valueMeta.getBigNumber(value);
        ((DecimalVector) vector).setSafe(index, bd);
        break;
      case IValueMeta.TYPE_DATE:
      case IValueMeta.TYPE_TIMESTAMP:
        Date date = valueMeta.getDate(value);
        if (vector instanceof DateMilliVector) {
          ((DateMilliVector) vector).setSafe(index, date.getTime());
        } else if (vector instanceof TimeStampMilliVector) {
          ((TimeStampMilliVector) vector).setSafe(index, date.getTime());
        }
        break;
      case IValueMeta.TYPE_BOOLEAN:
        ((BitVector) vector).setSafe(index, valueMeta.getBoolean(value) ? 1 : 0);
        break;
      case IValueMeta.TYPE_BINARY:
        byte[] bytes = valueMeta.getBinary(value);
        ((VarBinaryVector) vector).setSafe(index, bytes);
        break;
      default:
        ((VarCharVector) vector).setSafe(index, new Text(valueMeta.getString(value)));
    }
  }

  /**
   * Get value from Arrow vector
   */
  private static Object getVectorValue(FieldVector vector, int index, IValueMeta valueMeta) {
    if (vector.isNull(index)) {
      return null;
    }
    
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        return ((VarCharVector) vector).getObject(index).toString();
      case IValueMeta.TYPE_INTEGER:
        return ((BigIntVector) vector).get(index);
      case IValueMeta.TYPE_NUMBER:
        return ((Float8Vector) vector).get(index);
      case IValueMeta.TYPE_BIGNUMBER:
        return ((DecimalVector) vector).getObject(index);
      case IValueMeta.TYPE_DATE:
        if (vector instanceof DateMilliVector) {
          return new Date(((DateMilliVector) vector).get(index));
        } else if (vector instanceof TimeStampMilliVector) {
          return new Date(((TimeStampMilliVector) vector).get(index));
        }
        return null;
      case IValueMeta.TYPE_TIMESTAMP:
        return new Date(((TimeStampMilliVector) vector).get(index));
      case IValueMeta.TYPE_BOOLEAN:
        return ((BitVector) vector).get(index) == 1;
      case IValueMeta.TYPE_BINARY:
        return ((VarBinaryVector) vector).get(index);
      default:
        return vector.getObject(index).toString();
    }
  }

  /**
   * Clean up Arrow resources
   */
  public static void cleanup() {
    allocator.close();
  }
}