# Upgrade Notes for Hop CPython Plugin to Hop 2.13.0-rc1

This document outlines the changes required to upgrade the Hop CPython plugin to be compatible with Apache Hop 2.13.0-rc1.

## 1. Dependency Updates

### pom.xml Changes

```xml
<!-- Update Hop version -->
<hop.version>2.13.0-rc1</hop.version>

<!-- Update project version -->
<version>2.13.0-GA</version>

<!-- Update Java target/source version -->
<maven.compiler.target>17</maven.compiler.target>
<maven.compiler.source>17</maven.compiler.source>

<!-- Update test dependencies -->
<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.13.2</version>
    <scope>test</scope>
</dependency>

<!-- Consider updating Jackson (optional but recommended) -->
<jackson.version>2.16.1</jackson.version>
```

## 2. Code Changes

### Add @HopMetadataProperty Annotations

Add the Hop metadata property annotations to the fields in `CPythonScriptExecutorMeta.java`:

```java
@HopMetadataProperty
protected String m_pythonCommand = "";

@HopMetadataProperty
protected String m_pyPathEntries = "";

@HopMetadataProperty
protected String m_serverID = "";

@HopMetadataProperty
protected boolean m_loadScriptAtRuntime;

@HopMetadataProperty
protected String m_loadScriptFile = "";

@HopMetadataProperty
protected List<String> m_frameNames = new ArrayList<>();

@HopMetadataProperty
protected List<String> m_pyVarsToGet = new ArrayList<>();

@HopMetadataProperty
protected boolean m_includeRowIndex;

@HopMetadataProperty
protected boolean m_continueOnUnsetVars;

@HopMetadataProperty
protected String m_rowsToProcess = DEFAULT_ROWS_TO_PROCESS;

@HopMetadataProperty
protected String m_rowsToProcessSize = "";

@HopMetadataProperty
protected boolean m_doingReservoirSampling = false;

@HopMetadataProperty
protected String m_reservoirSamplingSize = "";

@HopMetadataProperty
protected String m_seed = "1";

@HopMetadataProperty
protected boolean m_includeInputAsOutput = false;

@HopMetadataProperty
protected String m_script = BaseMessages.getString(PKG, "CPythonScriptExecutorMeta.InitialScriptText");
```

### Update Log Usage

If any code is directly accessing the `log` object from BaseTransformMeta, update it to use `getLog()` instead:

```java
// Change from:
log.logBasic("Message");

// To:
getLog().logBasic("Message");
```

### Update Method Signatures (if necessary)

Update any method signatures that have changed in the Hop API, particularly in these methods:

- Check the `getFields()` method for potential signature changes
- Review `loadXml()` and `getXml()` methods which are transitioning to using the metadata properties

## 3. Testing

After making these changes:

1. Compile the project: `mvn clean compile`
2. Build the project: `mvn clean package`
3. Install in a Hop 2.13.0-rc1 environment and test:
   - Basic functionality
   - Script execution
   - Data conversion between Hop and Python
   - UI dialogs

## 4. Potential Issues

- Thread management in the UI: If the plugin performs background operations, ensure thread safety with newer SWT
- XML serialization: The plugin uses extensive XML operations that may need review
- Generic parameter usage: Verify generic type parameters in transform implementations match the API expectations

## 5. Future Recommendations

- Consider migrating from manual XML serialization to relying on the HopMetadataProperty annotations
- Update test cases to work with JUnit 5 (or newer JUnit 4)
- Review and modernize the Jackson dependency usage