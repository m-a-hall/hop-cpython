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

package org.phalanxdev.hop.pipeline.transforms.cpython;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.phalanxdev.hop.ui.pipeline.transforms.cpython.CPythonScriptExecutorDialog;
import org.phalanxdev.python.PythonSession;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta class for the CPythonScriptExecutor step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
@Transform(id = "CPythonScriptExecutor", image = "pylogo.png", name = "CPython Script Executor", description = "Executes a python script", categoryDescription = "Statistics")
public class CPythonScriptExecutorMeta extends BaseTransformMeta implements
    ITransformMeta<CPythonScriptExecutor, CPythonScriptExecutorData> {

  private static Class<?> PKG = CPythonScriptExecutor.class;

  protected static final String ROWS_TO_PROCESS_TAG = "rows_to_process";
  protected static final String ROWS_TO_PROCESS_SIZE_TAG = "rows_to_process_size";
  protected static final String RESERVOIR_SAMPLING_TAG = "reservoir_sampling";
  protected static final String RESERVOIR_SAMPLING_SIZE_TAG = "reservoir_sampling_size";
  protected static final String RESERVOIR_SAMPLING_SEED_TAG = "reservoir_sampling_seed";
  protected static final String INCLUDE_INPUT_AS_OUTPUT_TAG = "include_input_as_output";
  protected static final String INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG = "include_frame_row_index";
  protected static final String LOAD_SCRIPT_AT_RUNTIME_TAG = "load_script_at_runtime";
  protected static final String SCRIPT_TO_LOAD_TAG = "script_to_load";
  protected static final String SCRIPT_TAG = "py_script";
  protected static final String FRAME_NAMES_TAG = "frame_names";
  protected static final String PY_VARS_TO_GET_TAG = "py_vars_to_get";
  protected static final String CONTINUE_ON_UNSET_VARS_TAG = "continue_on_unset_vars";
  protected static final String SINGLE_FRAME_NAME_PREFIX_TAG = "frame_name";
  protected static final String INCOMING_STEP_NAMES_TAG = "incoming_step_names";
  protected static final String SINGLE_INCOMING_STEP_NAME_TAG = "step_name";
  protected static final String OUTPUT_FIELDS_TAG = "output_fields";
  protected static final String SINGLE_OUTPUT_FIELD_TAG = "output_field";

  /**
   * Default prefix for kettle data -> pandas frame name
   */
  public static final String DEFAULT_FRAME_NAME_PREFIX = "kettle_data";

  /**
   * Default row handling strategy
   */
  public static final String
      DEFAULT_ROWS_TO_PROCESS =
      BaseMessages.getString(PKG,
          "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.AllEntry.Label");

  /**
   * The script to execute
   */
  protected String m_script = BaseMessages
      .getString(PKG, "CPythonScriptExecutorMeta.InitialScriptText");

  /**
   * Whether to load a script at runtime
   */
  protected boolean m_loadScriptAtRuntime;

  /**
   * The script to load (if loading at runtime)
   */
  protected String m_loadScriptFile = ""; //$NON-NLS-1$

  /**
   * The name(s) of the data frames to create in python - one corresponding to each incoming row
   * set
   */
  protected List<String> m_frameNames = new ArrayList<>();

  /**
   * List of variables to get from python. This should hold exactly one variable in the case of
   * extracting a data frame. There can be more than one if all variables are either strings or
   * images
   */
  protected List<String> m_pyVarsToGet = new ArrayList<>();

  /**
   * Whether to include the pandas frame row index as an output field (when retrieving a single data
   * frame from python as output.
   */
  protected boolean m_includeRowIndex;

  /**
   * Whether to continue processing if one or more requested variables are not set in the python
   * environment after executing the script.
   */
  protected boolean m_continueOnUnsetVars;

  /**
   * Default Rows to Process
   */
  protected String m_rowsToProcess = DEFAULT_ROWS_TO_PROCESS;

  /**
   * Number of rows to process if <code>rows to process is batch </code>
   */
  protected String m_rowsToProcessSize = "";

  /**
   * True if reservoir sampling is to be used, in which case the batch size is the reservoir size
   */
  protected boolean m_doingReservoirSampling = false;

  /**
   * If reservoir sampling is enabled, this value is used to define the sampling size
   */
  protected String m_reservoirSamplingSize = "";

  /**
   * Random seed for reservoir sampling
   */
  protected String m_seed = "1"; //$NON-NLS-1$

  /**
   * True if input stream values should be copied to the output stream. Only applies when output is
   * a single pandas data frame; furthermore, number of output rows must match number of input
   * rows.
   */
  protected boolean m_includeInputAsOutput = false;

  /**
   * Outgoing fields
   */
  protected IRowMeta m_outputFields;

  /**
   * Get the output structure
   *
   * @return the output structure
   */
  public IRowMeta getOutputFields() {
    return m_outputFields;
  }

  /**
   * Set the output structure
   *
   * @param rm the output structure
   */
  public void setOutputFields(IRowMeta rm) {
    m_outputFields = rm;
  }

  public void setRowsToProcess(String s) {
    m_rowsToProcess = s;
  }

  public String getRowsToProcess() {
    return m_rowsToProcess;
  }

  public void setRowsToProcessSize(String s) {
    m_rowsToProcessSize = s;
  }

  public String getRowsToProcessSize() {
    return m_rowsToProcessSize;
  }

  /**
   * Set whether reservoir sampling is to be used in the single input case. Sampling is always used
   * when there are multiple input row sets.
   *
   * @param r true if reservoir sampling is to be used
   */
  public void setDoingReservoirSampling(boolean r) {
    m_doingReservoirSampling = r;
  }

  /**
   * Get whether reservoir sampling is to be used in the single input case. Sampling is always used
   * when there are multiple input row sets.
   *
   * @return true if reservoir sampling is to be used in the single input case.
   */
  public boolean getDoingReservoirSampling() {
    return m_doingReservoirSampling;
  }

  /**
   * Set the size of the reservoir
   *
   * @param s the size of the reservoir
   */
  public void setReservoirSamplingSize(String s) {
    m_reservoirSamplingSize = s;
  }

  /**
   * Get the size of the reservoir
   *
   * @return the size of the reservoir
   */
  public String getReservoirSamplingSize() {
    return m_reservoirSamplingSize;
  }

  /**
   * Set the random seed to use for reservoir sampling
   *
   * @param seed the random seed to use when reservoir sampling
   */
  public void setRandomSeed(String seed) {
    m_seed = seed;
  }

  /**
   * Get the random seed to use for reservoir sampling
   *
   * @return the random seed to use when reservoir sampling
   */
  public String getRandomSeed() {
    return m_seed;
  }

  /**
   * Sets whether the step should or not include input values in the output stream
   */
  public void setIncludeInputAsOutput(boolean s) {
    m_includeInputAsOutput = s;
  }

  /**
   * Gets whether the step should or not include input values in the output stream
   *
   * @return true if step should include input values in output
   */
  public boolean getIncludeInputAsOutput() {
    return m_includeInputAsOutput;
  }

  /**
   * Set whether to load a script from the file system at runtime rather than executing the user
   * supplied script
   *
   * @param l true if a script is to be loaded at runtime
   */
  public void setLoadScriptAtRuntime(boolean l) {
    m_loadScriptAtRuntime = l;
  }

  /**
   * Get whether to load a script from the file system at runtime rather than executing the user
   * supplied script
   *
   * @return true if a script is to be loaded at runtime
   */
  public boolean getLoadScriptAtRuntime() {
    return m_loadScriptAtRuntime;
  }

  /**
   * Set the path to the script to load at runtime (if loading at runtime)
   *
   * @param scriptFile the script file to load at runtime
   */
  public void setScriptToLoad(String scriptFile) {
    m_loadScriptFile = scriptFile;
  }

  /**
   * Get the path to the script to load at runtime (if loading at runtime)
   *
   * @return the script file to load at runtime
   */
  public String getScriptToLoad() {
    return m_loadScriptFile;
  }

  /**
   * Set the python script to execute
   *
   * @param script the script to execute
   */
  public void setScript(String script) {
    m_script = script;
  }

  /**
   * Get the python script to execute
   *
   * @return the script to execute
   */
  public String getScript() {
    return m_script;
  }

  /**
   * Set the frame names to use when converting incoming row sets into pandas data frames in python.
   * These are the variable names that the user can reference the data by
   *
   * @param names a list of frame names to use - one for each incoming row set
   */
  public void setFrameNames(List<String> names) {
    m_frameNames = names;
  }

  /**
   * Get the frame names to use when converting incoming row sets into pandas data frames in python.
   * These are the variable names that the user can reference the data by
   *
   * @return a list of frame names to use - one for each incoming row set
   */
  public List<String> getFrameNames() {
    return m_frameNames;
  }

  /**
   * Set the list of python variables to retrieve. If there is more than one variable being
   * retrieved, then each variable will be extracted from python as a string, unless it is an image,
   * in which case the image data is retrieved. The names of fields output by the step are expected
   * to match the variable names in this case; furthermore, the user is expected to set the
   * appropriate outgoing Kettle field type (this must be binary in the case of image data). Note
   * that the step will not know the types of the specified variables before runtime.
   * <p/>
   * If there is just one variable being extracted from python, then the output fields must match
   * the names of the columns of a pandas data frame (in the case that the variable is a data
   * frame), or the name of the variable in the case that is not a data frame. In both cases,
   * appropriate Kettle types must be specified by the user.
   *
   * @param pyVars the list of python variables to retrieve
   */
  public void setPythonVariablesToGet(List<String> pyVars) {
    m_pyVarsToGet = pyVars;
  }

  /**
   * Get the list of python variables to retrieve. If there is more than one variable being
   * retrieved, then each variable will be extracted from python as a string, unless it is an image,
   * in which case the image data is retrieved. The names of fields output by the step are expected
   * to match the variable names in this case; furthermore, the user is expected to set the
   * appropriate outgoing Kettle field type (this must be binary in the case of image data). Note
   * that the step will not know the types of the specified variables before runtime.
   * <p/>
   * If there is just one variable being extracted from python, then the output fields must match
   * the names of the columns of a pandas data frame (in the case that the variable is a data
   * frame), or the name of the variable in the case that is not a data frame. In both cases,
   * appropriate Kettle types must be specified by the user.
   *
   * @return the list of python variables to retrieve
   */
  public List<String> getPythonVariablesToGet() {
    return m_pyVarsToGet;
  }

  /**
   * Set whether to include the pandas data frame row index as an output field, in the case where
   * the output of the step is a single pandas data frame. Has no affect if multiple variables are
   * being retrieved from python.
   *
   * @param includeFrameRowIndexAsOutputField true to include the frame row index as an output
   * field
   */
  public void setIncludeFrameRowIndexAsOutputField(boolean includeFrameRowIndexAsOutputField) {
    m_includeRowIndex = includeFrameRowIndexAsOutputField;
  }

  /**
   * Get whether to include the pandas data frame row index as an output field, in the case where
   * the output of the step is a single pandas data frame. Has no affect if multiple variables are
   * being retrieved from python.
   *
   * @return true to include the frame row index as an output field
   */
  public boolean getIncludeFrameRowIndexAsOutputField() {
    return m_includeRowIndex;
  }

  /**
   * Set whether to continue in the case that one or more user specified variables to retrieve are
   * not set in the python environment after executing the script.
   *
   * @param continueOnUnset true to continue processing if there are unset variables
   */
  public void setContinueOnUnsetVars(boolean continueOnUnset) {
    m_continueOnUnsetVars = continueOnUnset;
  }

  /**
   * Get whether to continue in the case that one or more user specified variables to retrieve are
   * not set in the python environment after executing the script.
   *
   * @return true to continue processing if there are unset variables
   */
  public boolean getContinueOnUnsetVars() {
    return m_continueOnUnsetVars;
  }

  public IRowMeta determineOutputRowMeta(IRowMeta[] info, IVariables space)
      throws HopException {

    List<IRowMeta> incomingMetas = new ArrayList<>();
    IRowMeta rmi = new RowMeta();

    // possibly multiple incoming row sets
    for (IRowMeta r : info) {
      if (r != null) {
        incomingMetas.add(r);
      }
    }

    PythonSession.RowMetaAndRows
        scriptRM =
        CPythonScriptExecutorData
            .determineOutputMetaSingleVariable(this, incomingMetas, this, getLog(), space);

    return scriptRM.m_rowMeta;
  }

  @Override
  public void getFields(IRowMeta rowMeta, String transformName, IRowMeta[] info, TransformMeta nextTransform,
      IVariables space, IHopMetadataProvider metaStore) throws HopTransformException {

    rowMeta.clear();
    if (m_outputFields != null && m_outputFields.size() > 0) {
      rowMeta.addRowMeta(m_outputFields);

      // Check across all input fields to see if they are in the output, and
      // whether they are binary storage. If binary storage then copy over the original input value meta
      // (this is because get fields in the dialog just creates new ValueMetas without knowledge of storage type)
      if (getIncludeInputAsOutput()) {
        for (IRowMeta r : info) {
          if (r != null) {
            for (IValueMeta vm : r.getValueMetaList()) {
              int outIndex = m_outputFields.indexOfValue(vm.getName());
              if (outIndex >= 0) {
                m_outputFields.setValueMeta(outIndex, vm);
              }
            }
          }
        }
      }
    } else {
      int numRowMetas = 0;
      for (IRowMeta r : info) {
        if (r != null) {
          numRowMetas++;
        }
      }
      if (numRowMetas != m_frameNames.size()) {
        throw new HopTransformException(BaseMessages
            .getString(PKG, "CPythonScriptExecutorMeta.Error.IncorrectNumberOfIncomingStreams",
                m_frameNames.size(),
                numRowMetas));
      }

      // incoming fields
      addAllIncomingFieldsToOutput(rowMeta, transformName, info);

      // script fields
      try {
        addScriptFieldsToOutput(rowMeta, info, transformName, space);
      } catch (HopException ex) {
        throw new HopTransformException(ex);
      }
    }
  }

  /**
   * Add all incoming fields to the output row meta in the case where no output fields have been
   * defined/edited by the user
   */
  private void addAllIncomingFieldsToOutput(IRowMeta rowMeta, String transformName, IRowMeta[] info) {
    if (getIncludeInputAsOutput()) {
      for (IRowMeta r : info) {
        rowMeta.addRowMeta(r);
      }
    }
  }

  /**
   * Add script fields to output row meta in the case where no output fields have been
   * defined/edited by the user. If there is just one variable to extract from python, then the
   * script will be executed on some randomly generated data and the type of the variable will be
   * determined; if it is a pandas frame, then the field meta data can be determined.
   */
  private void addScriptFieldsToOutput(IRowMeta rowMeta, IRowMeta[] info, String transformName,
      IVariables space) throws HopException {
    if (m_pyVarsToGet.size() == 1) {
      // could be just a single pandas data frame - see if we can determine
      // the fields in this frame...
      IRowMeta scriptRM = determineOutputRowMeta(info, space);

      for (IValueMeta vm : scriptRM.getValueMetaList()) {
        vm.setOrigin(transformName);
        rowMeta.addValueMeta(vm);
      }
    } else {
      for (String varName : m_pyVarsToGet) {
        // IValueMeta vm = new ValueMeta( varName, IValueMeta.TYPE_STRING );
        IValueMeta vm = ValueMetaFactory.createValueMeta(varName, IValueMeta.TYPE_STRING);
        vm.setOrigin(transformName);
        rowMeta.addValueMeta(vm);
      }
    }
  }

  /**
   * Given a fully defined output row metadata structure, determine which of the output fields are
   * being copied from the input fields and which must be the output of the script.
   *
   * @param fullOutputRowMeta the fully defined output row metadata structure
   * @param scriptFields row meta that will hold script only fields
   * @param inputPresentInOutput row meta that will hold input fields being copied
   * @param infos the array of info row metas
   * @param transformName the name of the step
   */
  protected void determineInputFieldScriptFieldSplit(IRowMeta fullOutputRowMeta,
      IRowMeta scriptFields,
      IRowMeta inputPresentInOutput, IRowMeta[] infos, String transformName) {

    scriptFields.clear();
    inputPresentInOutput.clear();
    IRowMeta consolidatedInputFields = new RowMeta();
    for (IRowMeta r : infos) {
      consolidatedInputFields.addRowMeta(r);
    }

    for (IValueMeta vm : fullOutputRowMeta.getValueMetaList()) {
      int index = consolidatedInputFields.indexOfValue(vm.getName());
      if (index >= 0) {
        inputPresentInOutput.addValueMeta(vm);
      } else {
        // must be a script output (either a variable name field or data frame column name
        scriptFields.addValueMeta(vm);
      }
    }
  }

  @Override
  public void setDefault() {
    m_rowsToProcess =
        BaseMessages.getString(PKG,
            "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.AllEntry.Label");
    m_rowsToProcessSize = "";
    m_doingReservoirSampling = false;
    m_reservoirSamplingSize = "";
    m_frameNames = new ArrayList<>();
    m_continueOnUnsetVars = false;
    m_pyVarsToGet = new ArrayList<>();
    m_script = BaseMessages
        .getString(PKG, "CPythonScriptExecutorMeta.InitialScriptText"); //$NON-NLS-1$
  }

  @Override
  public ITransform createTransform(TransformMeta transformMeta,
      CPythonScriptExecutorData stepDataInterface, int i, PipelineMeta pipelineMeta,
      Pipeline trans) {
    return new CPythonScriptExecutor(transformMeta, this, stepDataInterface, i, pipelineMeta, trans);
  }

  @Override
  public CPythonScriptExecutorData getTransformData() {
    return new CPythonScriptExecutorData();
  }

  protected String varListToString() {
    StringBuilder b = new StringBuilder();
    for (String v : m_pyVarsToGet) {
      if (!org.apache.hop.core.util.Utils.isEmpty(v.trim())) {
        b.append(v.trim()).append(",");
      }
    }

    if (b.length() > 0) {
      b.setLength(b.length() - 1);
    }

    return b.toString();
  }

  protected void stringToVarList(String list) {
    m_pyVarsToGet.clear();
    String[] vars = list.split(",");
    for (String v : vars) {
      if (!org.apache.hop.core.util.Utils.isEmpty(v.trim())) {
        m_pyVarsToGet.add(v.trim());
      }
    }
  }

  @Override
  public String getXml() {
    StringBuilder buff = new StringBuilder();

    buff.append(XmlHandler.addTagValue(ROWS_TO_PROCESS_TAG, getRowsToProcess()));
    buff.append(XmlHandler.addTagValue(ROWS_TO_PROCESS_SIZE_TAG, getRowsToProcessSize()));
    buff.append(XmlHandler.addTagValue(RESERVOIR_SAMPLING_TAG, getDoingReservoirSampling()));
    buff.append(XmlHandler.addTagValue(RESERVOIR_SAMPLING_SIZE_TAG, getReservoirSamplingSize()));
    buff.append(XmlHandler.addTagValue(RESERVOIR_SAMPLING_SEED_TAG, getRandomSeed()));
    buff.append(XmlHandler.addTagValue(INCLUDE_INPUT_AS_OUTPUT_TAG, getIncludeInputAsOutput()));
    buff.append(XmlHandler.addTagValue(SCRIPT_TAG, getScript()));
    buff.append(XmlHandler.addTagValue(LOAD_SCRIPT_AT_RUNTIME_TAG, getLoadScriptAtRuntime()));
    buff.append(XmlHandler.addTagValue(SCRIPT_TO_LOAD_TAG, getScriptToLoad()));
    buff.append(XmlHandler.addTagValue(CONTINUE_ON_UNSET_VARS_TAG, getContinueOnUnsetVars()));
    buff.append(XmlHandler.addTagValue(PY_VARS_TO_GET_TAG, varListToString()));
    buff.append(
        XmlHandler.addTagValue(INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG,
            getIncludeFrameRowIndexAsOutputField()));

    // names of the frames to push into python
    buff.append("   " + XmlHandler.openTag(FRAME_NAMES_TAG)
        + Const.CR); //$NON-NLS-1$
    for (int i = 0; i < m_frameNames.size(); i++) {
      buff.append(
          "    " + XmlHandler
              .addTagValue(SINGLE_FRAME_NAME_PREFIX_TAG + i, m_frameNames.get(i))); //$NON-NLS-1$
    }
    buff.append("    " + XmlHandler.closeTag(FRAME_NAMES_TAG)
        + Const.CR); //$NON-NLS-1$

    // name of the corresponding step that is providing data for each frame
    buff.append("   " + XmlHandler.openTag(INCOMING_STEP_NAMES_TAG)
        + Const.CR); //$NON-NLS-1$
    List<IStream> infoStreams = getStepIOMeta().getInfoStreams();
    for (int i = 0; i < infoStreams.size(); i++) {
      buff.append("    " //$NON-NLS-1$
          + XmlHandler
          .addTagValue(SINGLE_INCOMING_STEP_NAME_TAG + i, infoStreams.get(i).getTransformName()));
    }
    buff.append("   " + XmlHandler.closeTag(INCOMING_STEP_NAMES_TAG)
        + Const.CR); //$NON-NLS-1$

    if (m_outputFields != null && m_outputFields.size() > 0) {
      buff.append("   " + XmlHandler.openTag(OUTPUT_FIELDS_TAG)
          + Const.CR); //$NON-NLS-1$
      for (int i = 0; i < m_outputFields.size(); i++) {
        IValueMeta vm = m_outputFields.getValueMeta(i);
        buff.append("        " + XmlHandler.openTag(SINGLE_OUTPUT_FIELD_TAG)
            + Const.CR); //$NON-NLS-1$
        buff.append("            " + XmlHandler.addTagValue("field_name", vm.getName())
            + Const.CR); //$NON-NLS-1$ //$NON-NLS-2$
        buff.append("            " + XmlHandler
            .addTagValue("type", vm.getTypeDesc())); //$NON-NLS-1$ //$NON-NLS-2$
        buff.append("        " + XmlHandler.closeTag(SINGLE_OUTPUT_FIELD_TAG)
            + Const.CR); //$NON-NLS-1$
      }
      buff.append("    " + XmlHandler.closeTag(OUTPUT_FIELDS_TAG)
          + Const.CR); //$NON-NLS-1$
    }

    return buff.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    String rowsToProcess = XmlHandler.getTagValue(transformNode, ROWS_TO_PROCESS_TAG);
    setRowsToProcess(rowsToProcess == null ? "" : rowsToProcess);
    String rowsToProcessSize = XmlHandler.getTagValue(transformNode, ROWS_TO_PROCESS_SIZE_TAG);
    setRowsToProcessSize(rowsToProcessSize == null ? "" : rowsToProcessSize);
    setDoingReservoirSampling(
        XmlHandler.getTagValue(transformNode, RESERVOIR_SAMPLING_TAG)
            .equalsIgnoreCase("Y")); //$NON-NLS-1$
    String reservoirSamplingSize = XmlHandler.getTagValue(transformNode, RESERVOIR_SAMPLING_SIZE_TAG);
    setReservoirSamplingSize(reservoirSamplingSize == null ? "" : reservoirSamplingSize);
    setRandomSeed(XmlHandler.getTagValue(transformNode, RESERVOIR_SAMPLING_SEED_TAG));
    String includeInputAsOutput = XmlHandler.getTagValue(transformNode, INCLUDE_INPUT_AS_OUTPUT_TAG);
    if (!org.apache.hop.core.util.Utils.isEmpty(includeInputAsOutput)) {
      setIncludeInputAsOutput(includeInputAsOutput.equalsIgnoreCase("Y")); //$NON-NLS-1$
    }
    String includeFrameRowIndex = XmlHandler
        .getTagValue(transformNode, INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG);
    if (!org.apache.hop.core.util.Utils.isEmpty(includeFrameRowIndex)) {
      setIncludeFrameRowIndexAsOutputField(includeFrameRowIndex.equalsIgnoreCase("Y"));
    }

    setScript(XmlHandler.getTagValue(transformNode, SCRIPT_TAG));

    String loadScript = XmlHandler.getTagValue(transformNode, LOAD_SCRIPT_AT_RUNTIME_TAG);
    if (!org.apache.hop.core.util.Utils.isEmpty(loadScript)) {
      setLoadScriptAtRuntime(loadScript.equalsIgnoreCase("Y")); //$NON-NLS-1$
    }
    setScriptToLoad(XmlHandler.getTagValue(transformNode, SCRIPT_TO_LOAD_TAG));

    String continueOnUnset = XmlHandler.getTagValue(transformNode, CONTINUE_ON_UNSET_VARS_TAG);
    if (!org.apache.hop.core.util.Utils.isEmpty(continueOnUnset)) {
      setContinueOnUnsetVars(continueOnUnset.equalsIgnoreCase("Y"));
    }

    String pyVars = XmlHandler.getTagValue(transformNode, PY_VARS_TO_GET_TAG);
    if (!org.apache.hop.core.util.Utils.isEmpty(pyVars)) {
      stringToVarList(pyVars);
    }

    // get the frame names
    Node frameNameFields = XmlHandler.getSubNode(transformNode, FRAME_NAMES_TAG);
    if (frameNameFields != null) {
      Node frameNode = null;
      int i = 0;
      while ((frameNode = XmlHandler.getSubNode(frameNameFields, SINGLE_FRAME_NAME_PREFIX_TAG + i))
          != null) {
        m_frameNames.add(XmlHandler.getNodeValue(frameNode));
        i++;
      }
    }

    // get the step names
    Node stepNameFields = XmlHandler.getSubNode(transformNode, INCOMING_STEP_NAMES_TAG);
    if (stepNameFields != null) {
      List<IStream> infoStreams = getStepIOMeta().getInfoStreams();

      for (int i = 0; i < infoStreams.size(); i++) {
        Node stepNameNode = XmlHandler
            .getSubNode(stepNameFields, SINGLE_INCOMING_STEP_NAME_TAG + i);
        infoStreams.get(i).setSubject(XmlHandler.getNodeValue(stepNameNode));
      }
    }

    // get the outgoing fields
    Node outgoingFields = XmlHandler.getSubNode(transformNode, OUTPUT_FIELDS_TAG);
    if (outgoingFields != null
        && XmlHandler.countNodes(outgoingFields, SINGLE_OUTPUT_FIELD_TAG) > 0) {
      int nrfields = XmlHandler.countNodes(outgoingFields, SINGLE_OUTPUT_FIELD_TAG);

      m_outputFields = new RowMeta();
      for (int i = 0; i < nrfields; i++) {
        Node fieldNode = XmlHandler.getSubNodeByNr(outgoingFields, SINGLE_OUTPUT_FIELD_TAG, i);
        String name = XmlHandler.getTagValue(fieldNode, "field_name"); //$NON-NLS-1$
        String type = XmlHandler.getTagValue(fieldNode, "type"); //$NON-NLS-1$
        //IValueMeta vm = new ValueMeta( name, ValueMeta.getType( type ) );
        try {
          IValueMeta vm = ValueMetaFactory
              .createValueMeta(name, ValueMetaFactory.getIdForValueMeta(type));
          m_outputFields.addValueMeta(vm);
        } catch (HopPluginException ex) {
          throw new HopXmlException(ex);
        }
      }
    }
  }

  @Override
  public Object clone() {
    CPythonScriptExecutorMeta retval = (CPythonScriptExecutorMeta) super.clone();

    return retval;
  }

  public void clearStepIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta();
    ( (TransformIOMeta) ioMeta ).clearStreams();
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset
  }

  public ITransformIOMeta getStepIOMeta() {

    ITransformIOMeta ioMeta = super.getTransformIOMeta();
    if (ioMeta.getTargetStreams().isEmpty()) {
      ((TransformIOMeta) ioMeta).setInputAcceptor(true);
      ((TransformIOMeta) ioMeta).setOutputProducer(true);
      ((TransformIOMeta) ioMeta).setInputOptional(true);
      ((TransformIOMeta) ioMeta).setSortedDataRequired(false);
      ioMeta.setInputDynamic(false);
      ioMeta.setOutputDynamic(false);

      int numExpectedStreams = m_frameNames.size();

      for (int i = 0; i < numExpectedStreams; i++) {
        ioMeta.addStream(
            new Stream(IStream.StreamType.INFO, null, "Input to pandas frame " + (i + 1),
                StreamIcon.INFO, null));
      }
    }
    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> steps) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();

    for (IStream stream : targetStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(steps, (String) stream.getSubject()));
    }
  }

  @Override
  public String getDialogClassName() {
    return CPythonScriptExecutorDialog.class.getCanonicalName();
  }
}
