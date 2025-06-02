# Apache Arrow Migration Plan for Hop CPython Plugin

## Overview
This document outlines the migration from CSV/JSON-based data transfer to Apache Arrow IPC format for improved performance, reliability, and type safety.

## Benefits
1. **Eliminates encoding issues** - No more UTF-8/Base64 workarounds
2. **Better performance** - Zero-copy data transfer, columnar format
3. **Extended type support** - Native support for all Hop data types
4. **Simpler codebase** - Remove CSV parsing/escaping logic
5. **Direct pandas integration** - Built-in Arrow-pandas conversion

## Architecture Changes

### Current Architecture
```
Java (Hop) <--> Socket (CSV/JSON) <--> Python (pandas)
```

### New Architecture
```
Java (Hop) <--> Socket (Arrow IPC) <--> Python (pandas via PyArrow)
```

## Implementation Plan

### Phase 1: Dependencies and Core Infrastructure

#### 1.1 Add Maven Dependencies (pom.xml)
```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>15.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-netty</artifactId>
    <version>15.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-algorithm</artifactId>
    <version>15.0.0</version>
</dependency>
```

#### 1.2 Update Python Requirements
- Add `pyarrow>=15.0.0` to requirements
- Update pyCheck.py to verify pyarrow installation

### Phase 2: Protocol Updates

#### 2.1 Message Protocol
- Keep length-delimited protocol for compatibility
- Replace CSV payload with Arrow IPC stream format
- Update command structure to include Arrow metadata

#### 2.2 New Protocol Flow
1. **Command Phase**: JSON command (unchanged)
2. **Data Phase**: Arrow RecordBatch stream instead of CSV
3. **Response Phase**: Arrow RecordBatch for DataFrames

### Phase 3: Java Implementation

#### 3.1 ServerUtils.java Updates
- Add `sendArrowData()` method to replace CSV sending
- Add `receiveArrowData()` method to replace CSV receiving
- Create Arrow schema from Hop RowMeta
- Convert Hop rows to Arrow VectorSchemaRoot

#### 3.2 Type Mapping
| Hop Type | Arrow Type |
|----------|------------|
| String | Utf8 |
| Integer | Int64 |
| Number | Float64 |
| BigNumber | Decimal128 |
| Date | Date64 |
| Timestamp | Timestamp(ms) |
| Boolean | Bool |
| Binary | Binary |

#### 3.3 PythonSession.java Updates
- Update `rowsFromPythonDataFrame()` to use Arrow
- Update `pushRowsToPandasDataFrame()` to use Arrow
- Add Arrow resource management (BufferAllocator)

### Phase 4: Python Implementation

#### 4.1 pyServer.py Updates
- Add Arrow IPC reader/writer functions
- Replace CSV parsing with Arrow table operations
- Update DataFrame conversion methods

#### 4.2 Key Functions to Update
```python
def receive_arrow_data(socket):
    """Receive Arrow IPC stream from Java"""
    reader = pa.ipc.RecordBatchStreamReader(socket)
    table = reader.read_all()
    return table.to_pandas()

def send_arrow_data(socket, df):
    """Send pandas DataFrame as Arrow IPC stream"""
    table = pa.Table.from_pandas(df)
    writer = pa.ipc.RecordBatchStreamWriter(socket, table.schema)
    writer.write_table(table)
    writer.close()
```

### Phase 5: Compatibility and Migration

#### 5.1 Feature Flag
- Add configuration option: `useArrowTransfer` (default: true)
- Allow fallback to CSV for compatibility

#### 5.2 Version Detection
- Check pyarrow availability at runtime
- Automatic fallback if pyarrow not installed

### Phase 6: Testing

#### 6.1 Unit Tests
- Test Arrow data conversion for all Hop types
- Test null handling
- Test large datasets (>1M rows)
- Test Unicode/special characters

#### 6.2 Integration Tests
- Test with existing sample pipeline
- Test error handling and recovery
- Test memory management
- Performance benchmarks

#### 6.3 Backward Compatibility Tests
- Test with CSV fallback mode
- Test with older Python environments

### Phase 7: Documentation

#### 7.1 Update README.md
- Add pyarrow to requirements
- Document performance improvements
- Add migration guide

#### 7.2 Update CLAUDE.md
- Document new Arrow architecture
- Update data flow diagram

## Risk Mitigation

1. **Dependency Size**: Arrow adds ~20MB to plugin size
   - Mitigation: Document size increase, provide "lite" version if needed

2. **Python Environment**: Users need pyarrow installed
   - Mitigation: Automatic fallback to CSV, clear error messages

3. **Memory Usage**: Arrow uses off-heap memory
   - Mitigation: Proper resource cleanup, configurable memory limits

## Success Criteria

1. All existing functionality works with Arrow
2. UTF-8 encoding issues resolved
3. Performance improvement for datasets >10k rows
4. Clean fallback to CSV when needed
5. All tests passing

## Timeline Estimate

- Phase 1-2: 2 hours (dependencies, protocol design)
- Phase 3-4: 4 hours (core implementation)
- Phase 5: 2 hours (compatibility layer)
- Phase 6: 3 hours (testing)
- Phase 7: 1 hour (documentation)

Total: ~12 hours of development