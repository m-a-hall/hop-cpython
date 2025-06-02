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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;

/**
 * Class implementing a session for interacting with Python
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 */
public class PythonSession {

  /**
   * Hop variable key to specify the full path to the default python command. Takes precedence over both the java
   * property and the system evnironment variable
   */
  public static final String HOP_CPYTHON_COMMAND_PROPERTY_KEY = "hop.cpython.command";

  /**
   * Java property to specify full path to the default python command. If not set (and env var not set), we assume 'python' is in
   * the PATH. Takes precedence over the env var.
   */
  public static final String CPYTHON_COMMAND_PROPERTY_KEY = "cpython.command";

  /**
   * System environment variable to specify full path to the default python command. If not set (and java prop is not set), we
   * assume 'python' is in the PATH.
   */
  public static final String CPYTHON_COMMAND_ENV_VAR_KEY = "HOP_CPYTHON_COMMAND";

  public static enum PythonVariableType {
    DataFrame, Image, String, Unknown;
  }

  /**
   * Simple container for row meta data and rows
   *
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   */
  public static class RowMetaAndRows {
    public Object[][] m_rows;
    public IRowMeta m_rowMeta;
  }

  /**
   * The command used to start python for this session.
   */
  private String m_pythonCommand;

  /**
   * Static map of initialized servers. A "default" entry will be used for
   * backward compatibility
   */
  private static Map<String, PythonSession>
      m_pythonServers =
      Collections.synchronizedMap( new HashMap<String, PythonSession>() );

  private static Map<String, String>
      m_pythonEnvCheckResults =
      Collections.synchronizedMap( new HashMap<String, String>() );

  /**
   * The unique key for this session/server
   */
  private String m_sessionKey;

  /**
   * the current session holder
   */
  private Object m_sessionHolder;

  /**
   * The session singleton
   */
  private static PythonSession s_sessionSingleton;

  /**
   * The results of the python check script
   */
  private static String s_pythonEnvCheckResults = "";

  /**
   * For locking
   */
  protected SessionMutex m_mutex = new SessionMutex();

  /**
   * Server socket
   */
  protected ServerSocket m_serverSocket;

  /**
   * Local socket for comms with the python server
   */
  protected Socket m_localSocket;

  /**
   * The process executing the server
   */
  protected Process m_serverProcess;

  /**
   * True when the server has been shutdown
   */
  protected boolean m_shutdown;

  /**
   * A shutdown hook for stopping the server
   */
  protected Thread m_shutdownHook;

  /**
   * PID of the running python server
   */
  protected int m_pythonPID = -1;

  /**
   * The log to use
   */
  protected ILogChannel m_log;

  /**
   * Path to the tmp directory
   */
  protected static String s_osTmpDir = null;
  protected static boolean s_attemptedScriptInstall;

  /**
   * Whether Arrow support is available in Python
   */
  protected boolean m_arrowAvailable = false;

  /**
   * Whether to use Arrow for data transfer (if available)
   */
  protected boolean m_useArrow = true;

  /**
   * Acquire the session for the requester
   *
   * @param requester the object requesting the session
   * @return the session singleton
   * @throws SessionException if python is not available
   */
  public static PythonSession acquireSession( Object requester ) throws SessionException {
    if ( s_sessionSingleton == null ) {
      throw new SessionException( "Python not available!" );
    }

    return s_sessionSingleton.getSession( requester );
  }

  /**
   * @param pythonCommand command (either fully qualified path or that which is
   *                      in the PATH). This, plus the optional ownerID is used to lookup
   *                      and return a session/server.
   * @param ownerID       an optional ownerID string for acquiring the session. This
   *                      can be used to restrict the session/server to one (or more)
   *                      clients (i.e. those with the ID "ownerID").
   * @param requester     the object requesting the session/server
   * @return a session
   * @throws SessionException if the requested session/server is not available (or
   *                          does not exist).
   */
  public static PythonSession acquireSession( String pythonCommand, String ownerID, Object requester )
      throws SessionException {
    String key = pythonCommand + ( ownerID != null && ownerID.length() > 0 ? ownerID : "" );
    if ( !m_pythonServers.containsKey( key ) ) {
      throw new SessionException( "Python session " + key + " does not seem to exist!" );
    }
    return m_pythonServers.get( key ).getSession( requester );
  }

  /**
   * Release the session so that other clients can obtain it. This method does
   * nothing if the requester is not the current session holder
   *
   * @param requester the session holder
   */
  public static void releaseSession( Object requester ) {
    s_sessionSingleton.dropSession( requester );
  }

  /**
   * Release the user-specified python session.
   *
   * @param pythonCommand command (either fully qualified path or that which is
   *                      in the PATH). This, plus the optional ownerID is used to lookup a
   *                      session/server.
   * @param ownerID       an optional ownerID string for identifying the session. This
   *                      can be used to restrict the session/server to one (or more)
   *                      clients (i.e. those with the ID "ownerID").
   * @param requester     the object requesting the session/server
   * @throws SessionException if the requested session/server is not available (or
   *                          does not exist).
   */
  public static void releaseSession( String pythonCommand, String ownerID, Object requester ) throws SessionException {
    String key = pythonCommand + ( ownerID != null && ownerID.length() > 0 ? ownerID : "" );
    if ( !m_pythonServers.containsKey( key ) ) {
      throw new SessionException( "Python session " + key + " does not seem to exist!" );
    }
    m_pythonServers.get( key ).dropSession( requester );
  }

  /**
   * Returns true if the python environment/server is available
   *
   * @return true if the python environment/server is available
   */
  public static boolean pythonAvailable() {
    return s_sessionSingleton != null;
  }

  /**
   * Returns true if the user-specified python environment/server (as specified
   * by pythonCommand (and optional ownerID) is available.
   *
   * @param pythonCommand command (either fully qualified path or that which is
   *                      in the PATH). This, plus the optional ownerID is used to lookup a
   *                      session/server.
   * @param ownerID       an optional ownerID string for identifying the session. This
   *                      can be used to restrict the session/server to one (or more)
   *                      clients (i.e. those with the ID "ownerID").
   */
  public static synchronized boolean pythonAvailable( String pythonCommand, String ownerID ) {
    String key = pythonCommand + ( ownerID != null && ownerID.length() > 0 ? ownerID : "" );
    return m_pythonServers.containsKey( key );
  }

  protected static synchronized File installPyScriptsToTmp() throws IOException {
    if ( s_osTmpDir != null ) {
      return new File( s_osTmpDir );
    }

    if ( !s_attemptedScriptInstall ) {

      ClassLoader loader = PythonSession.class.getClassLoader();
      InputStream in = loader.getResourceAsStream( "py/pyCheck.py" );
      if ( in == null ) {
        throw new IOException( "Unable to read the pyCheck.py script as a resource" );
      }

      String tmpDir = System.getProperty( "java.io.tmpdir" );
      // System.err.println( "******* System tmp dir: " + tmpDir );
      File tempDir = new File( tmpDir );
      String pyCheckDest = tmpDir + File.separator + "pyCheck.py";
      String pyServerDest = tmpDir + File.separator + "pyServer.py";

      PrintWriter outW = null;
      BufferedReader inR = null;
      try {
        outW = new PrintWriter( new BufferedWriter( new FileWriter( pyCheckDest ) ) );
        inR = new BufferedReader( new InputStreamReader( in ) );
        String line;
        while ( ( line = inR.readLine() ) != null ) {
          outW.println( line );
        }
        outW.flush();
        outW.close();
        inR.close();

        in = loader.getResourceAsStream( "py/pyServer.py" );
        outW = new PrintWriter( new BufferedWriter( new FileWriter( pyServerDest ) ) );
        inR = new BufferedReader( new InputStreamReader( in ) );
        while ( ( line = inR.readLine() ) != null ) {
          outW.println( line );
        }
      } catch ( IOException ex ) {
        ex.printStackTrace();
        throw ex;
      } finally {
        if ( outW != null ) {
          outW.flush();
          outW.close();
        }
        if ( inR != null ) {
          inR.close();
        }
        s_attemptedScriptInstall = true;
      }
      s_osTmpDir = tmpDir;
      return tempDir;
    } else {
      throw new IOException( "Unable to install python scripts" );
    }
  }

  /**
   * Generates a script (shell or batch) by which to execute a given python script.
   *
   * @param pathEntries  additional PATH entries needed for python to execute correctly
   * @param pyScriptName the name of the script (in wekaPython/resources/py) to execute
   * @param windows      true if we are running under Windows
   * @param scriptArgs   optional arguments for the python script
   * @return the generated sh/bat script
   */
  private String getLaunchScript( String pathEntries, String pyScriptName, boolean windows, Object... scriptArgs ) {
    File tmp = new File( s_osTmpDir );
    String script = tmp.toString() + File.separator + pyScriptName;

    String pathOriginal = System.getenv( windows ? "Path" : "PATH" );
    if ( pathOriginal == null ) {
      pathOriginal = "";
    }

    File pythFile = new File( m_pythonCommand );
    String exeDir = pythFile.getParent() != null ? pythFile.getParent().toString() : "";

    String
        finalPath =
        pathEntries != null && pathEntries.length() > 0 ? pathEntries + File.pathSeparator + pathOriginal :
            pathOriginal;

    finalPath = exeDir.length() > 0 ? exeDir + File.pathSeparator + finalPath : "" + finalPath;

    StringBuilder sbuilder = new StringBuilder();
    if ( windows ) {
      sbuilder.append( "@echo off" ).append( "\n\n" );
      sbuilder.append( "PATH=" + finalPath ).append( "\n\n" );
      sbuilder.append( "python " + script );
    } else {
      sbuilder.append( "#!/bin/sh" ).append( "\n\n" );
      sbuilder.append( "export PATH=" + finalPath ).append( "\n\n" );
      sbuilder.append( "python " + script );
    }

    for ( Object arg : scriptArgs ) {
      sbuilder.append( " " ).append( arg.toString() );
    }
    sbuilder.append( "\n" );

    return sbuilder.toString();
  }

  /**
   * Executes the python environment check script via a wrapping shell/batch script.
   *
   * @param pathEntries additional entries for the PATH that are required in order for
   *                    python to execute correctly
   * @return the result of executing the python environment check
   * @throws IOException if a problem occurs
   */
  private String writeAndLaunchPyCheck( String pathEntries ) throws IOException {

    String osType = System.getProperty( "os.name" );
    boolean windows = osType != null && osType.toLowerCase().contains( "windows" );

    String script = getLaunchScript( pathEntries, "pyCheck.py", windows );

    // System.err.println("**** Executing shell script: \n\n" + script);

    String scriptPath = File.createTempFile( "nixtester_", windows ? ".bat" : ".sh" ).toString();
    // System.err.println("Script path: " + scriptPath);

    FileWriter fwriter = new FileWriter( scriptPath );
    fwriter.write( script );
    fwriter.flush();
    fwriter.close();

    if ( !windows ) {
      Runtime.getRuntime().exec( "chmod u+x " + scriptPath );
    }
    ProcessBuilder builder = new ProcessBuilder( scriptPath );
    Process pyProcess = builder.start();
    StringWriter writer = new StringWriter();
    IOUtils.copy( pyProcess.getInputStream(), writer );

    return writer.toString();
  }

  /**
   * Launches the server using a shell/batch script generated on the fly. Used
   * when the user supplies a path to a python executable and additional entries
   * are needed in the PATH.
   *
   * @param pathEntries entries to prepend to the PATH
   * @throws IOException if a problem occurs when launching the server
   */
  private void launchServerScript( String pathEntries ) throws IOException {
    String osType = System.getProperty( "os.name" );
    boolean windows = osType != null && osType.toLowerCase().contains( "windows" );

    if ( m_log != null ) {
      m_log.logBasic( "Launching Python server with script (OS: " + osType + ")" );
    }

    Thread acceptThread = startServerSocket();
    boolean debug = m_log != null && m_log.isDebug();
    String
        script =
        getLaunchScript( pathEntries, "pyServer.py", windows, m_serverSocket.getLocalPort(), debug ? "debug" : "" );
    if ( debug ) {
      System.err.println( "Executing server launch script:\n\n" + script );
    }

    String scriptPath = File.createTempFile( "pyserver_", windows ? ".bat" : ".sh" ).toString();
    
    if ( m_log != null ) {
      m_log.logBasic( "Writing server launch script to: " + scriptPath );
    }

    FileWriter fwriter = new FileWriter( scriptPath );
    fwriter.write( script );
    fwriter.flush();
    fwriter.close();

    if ( !windows ) {
      if ( m_log != null ) {
        m_log.logDebug( "Making script executable: chmod u+x " + scriptPath );
      }
      Runtime.getRuntime().exec( "chmod u+x " + scriptPath );
    }

    ProcessBuilder processBuilder = new ProcessBuilder( scriptPath );
    
    if ( m_log != null ) {
      m_log.logBasic( "Starting Python server process..." );
    }
    
    m_serverProcess = processBuilder.start();
    
    // Start a thread to capture error output from the Python process
    Thread errorReaderThread = new Thread() {
      @Override public void run() {
        try {
          BufferedReader errorReader = new BufferedReader(
              new InputStreamReader( m_serverProcess.getErrorStream() ) );
          String line;
          while ( ( line = errorReader.readLine() ) != null ) {
            if ( m_log != null ) {
              m_log.logError( "Python server error: " + line );
            } else {
              System.err.println( "Python server error: " + line );
            }
          }
        } catch ( IOException e ) {
          // Process ended
        }
      }
    };
    errorReaderThread.setDaemon( true );
    errorReaderThread.start();
    
    if ( m_log != null ) {
      m_log.logBasic( "Waiting for Python server to connect on port " + m_serverSocket.getLocalPort() + "..." );
    }
    
    try {
      acceptThread.join();
    } catch ( InterruptedException e ) {
      if ( m_log != null ) {
        m_log.logError( "Interrupted while waiting for server connection" );
      }
    }

    checkLocalSocketAndCreateShutdownHook();
  }

  /**
   * Private constructor
   *
   * @param pythonCommand the python command to use
   * @param serverID      optional ID for the server. This, plus the path to the python executable can be
   *                      used to uniquely identify a given server instance
   * @param pathEntries   optional additional entries for the PATH that are required for this python
   *                      instance/virtual environment to execute correctly
   * @param defaultServer true if this is the default server (i.e. the python already present in the path)
   * @param log           the log to use
   * @throws IOException if a problem occurs
   */
  private PythonSession( String pythonCommand, String serverID, String pathEntries, boolean defaultServer,
      ILogChannel log ) throws IOException {

    m_log = log;
    
    if ( log != null ) {
      log.logBasic( "Initializing Python session with command: " + pythonCommand );
      if ( serverID != null && serverID.length() > 0 ) {
        log.logBasic( "Server ID: " + serverID );
      }
      if ( pathEntries != null && pathEntries.length() > 0 ) {
        log.logBasic( "Additional PATH entries: " + pathEntries );
      }
    }

    try {
      if ( log != null ) {
        log.logDebug( "Installing Python scripts to temp directory..." );
      }
      installPyScriptsToTmp();
    } catch ( IOException e ) {
      if ( log != null ) {
        log.logError( "Failed to install Python scripts to temp directory: " + e.getMessage() );
      }
      throw e;
    }
    m_pythonCommand = pythonCommand;
    String key = pythonCommand + ( serverID != null && serverID.length() > 0 ? serverID : "" );
    m_sessionKey = key;

    if ( PythonSession.m_pythonServers.containsKey( m_sessionKey ) ) {
      throw new IOException( "A server session for " + m_sessionKey + " Already exists!" );
    }
    if ( !defaultServer && pathEntries != null && pathEntries.length() > 0 ) {
      // use a shell/batch script to launch the server (so that we can
      // set the path so that the python server works correctly)
      if ( log != null ) {
        log.logBasic( "Running Python environment check with additional PATH entries..." );
      }
      String envCheckResults = writeAndLaunchPyCheck( pathEntries );
      m_pythonEnvCheckResults.put( m_sessionKey, envCheckResults );
      
      if ( log != null && envCheckResults.length() > 0 ) {
        log.logDetailed( "Python environment check results:\n" + envCheckResults );
      }
      
      // Check if environment check passed by looking for error indicators
      // Exclude informational messages about pyarrow availability 
      boolean hasErrors = (envCheckResults.contains("is not available") && 
                          !envCheckResults.contains("Apache Arrow support not available")) ||
                         envCheckResults.contains("does not meet") ||
                         envCheckResults.contains("problem occurred") ||
                         envCheckResults.contains("did not import correctly") ||
                         (envCheckResults.contains("Library ") && envCheckResults.contains("is not available"));
      
      if ( !hasErrors ) {
        // launch server
        log.logDetailed(
            "Python environment check passed. Launching " + m_pythonCommand + " with additional path [" + pathEntries + "] " + ( serverID != null ?
                " ID " + serverID : "" ) );
        launchServerScript( pathEntries );
        m_pythonServers.put( m_sessionKey, this );
      } else {
        log.logError( "Python environment check failed:\n\n" + envCheckResults );
      }
    } else {
      File tmp = new File( s_osTmpDir );
      String tester = tmp.toString() + File.separator + "pyCheck.py";

      if ( log != null ) {
        log.logBasic( "Running Python environment check using: " + pythonCommand + " " + tester );
      }

      ProcessBuilder builder = new ProcessBuilder( pythonCommand, tester );
      Process pyProcess = builder.start();
      StringWriter writer = new StringWriter();
      StringWriter errorWriter = new StringWriter();
      IOUtils.copy( pyProcess.getInputStream(), writer );
      IOUtils.copy( pyProcess.getErrorStream(), errorWriter );
      String envCheckResults = writer.toString();
      String errorOutput = errorWriter.toString();
      m_shutdown = false;

      if ( log != null && envCheckResults.length() > 0 ) {
        log.logDetailed( "Python environment check output:\n" + envCheckResults );
      }
      if ( log != null && errorOutput.length() > 0 ) {
        log.logError( "Python environment check errors:\n" + errorOutput );
      }

      m_pythonEnvCheckResults.put( m_sessionKey, envCheckResults );
      
      // Check if environment check passed by looking for error indicators
      // Exclude informational messages about pyarrow availability 
      boolean hasErrors = (envCheckResults.contains("is not available") && 
                          !envCheckResults.contains("Apache Arrow support not available")) ||
                         envCheckResults.contains("does not meet") ||
                         envCheckResults.contains("problem occurred") ||
                         envCheckResults.contains("did not import correctly") ||
                         (envCheckResults.contains("Library ") && envCheckResults.contains("is not available"));
      
      if ( !hasErrors ) {
        log.logDetailed( "Python environment check passed. Launching " + m_pythonCommand + ( serverID != null ? " ID " + serverID : "" ));
        // launch server
        launchServer( true );
        m_pythonServers.put( m_sessionKey, this );

        if ( s_sessionSingleton == null && defaultServer ) {
          s_sessionSingleton = this;
          s_pythonEnvCheckResults = envCheckResults;
        }
      } else {
        log.logError( "Python environment check failed:\n\n" + envCheckResults );
      }
    }
  }

  /**
   * Private constructor
   *
   * @param pythonCommand the command used to start python
   * @param log           the log to use
   * @throws IOException if a problem occurs
   */
  private PythonSession( String pythonCommand, ILogChannel log ) throws IOException {
    this( pythonCommand, null, null, true, log );
  }

  /**
   * Gets the access to python for a requester. Handles locking.
   *
   * @param requester the requesting object
   * @return the session
   * @throws SessionException if python is not available
   */
  private synchronized PythonSession getSession( Object requester ) throws SessionException {
    if ( m_sessionHolder == requester ) {
      return this;
    }

    m_mutex.safeLock();
    m_sessionHolder = requester;
    return this;
  }

  /**
   * Release the session for a requester
   *
   * @param requester the requesting object
   */
  private void dropSession( Object requester ) {
    if ( requester == m_sessionHolder ) {
      m_sessionHolder = null;
      m_mutex.unlock();
    }
  }

  /**
   * Starts the server socket.
   *
   * @return the Thread that the server socket is waiting for a connection on.
   * @throws IOException if a problem occurs
   */
  private Thread startServerSocket() throws IOException {
    if ( m_log != null ) {
      m_log.logBasic( "Creating server socket..." );
    }
    
    m_serverSocket = new ServerSocket( 0 );
    m_serverSocket.setSoTimeout( 12000 );
    
    int port = m_serverSocket.getLocalPort();
    if ( m_log != null ) {
      m_log.logBasic( "Server socket created on port " + port + " with timeout 12000ms" );
    }

    Thread acceptThread = new Thread() {
      @Override public void run() {
        try {
          if ( m_log != null ) {
            m_log.logDebug( "Waiting for Python server to connect on port " + port + "..." );
          }
          m_localSocket = m_serverSocket.accept();
          if ( m_log != null ) {
            m_log.logBasic( "Python server connected successfully" );
          }
        } catch ( IOException e ) {
          if ( m_log != null ) {
            m_log.logError( "Failed to accept connection from Python server: " + e.getMessage() );
          }
          m_localSocket = null;
        }
      }
    };
    acceptThread.start();

    return acceptThread;
  }

  /**
   * If the local socket could not be created, this method shuts down the
   * server. Otherwise, a shutdown hook is added to bring the server down when
   * the JVM exits.
   *
   * @throws IOException if a problem occurs
   */
  private void checkLocalSocketAndCreateShutdownHook() throws IOException {
    if ( m_localSocket == null ) {
      if ( m_log != null ) {
        m_log.logError( "Failed to establish connection with Python server - socket is null" );
      }
      shutdown();
      throw new IOException( "Was unable to start python server" );
    } else {
      if ( m_log != null ) {
        m_log.logBasic( "Connection established with Python server, receiving initialization response..." );
      }
      
      // Use the new method that also returns Arrow availability
      Map<String, Object> initResponse = ServerUtils.receiveServerInitResponse( m_localSocket.getInputStream() );
      m_pythonPID = (Integer) initResponse.get( "pid" );
      m_arrowAvailable = (Boolean) initResponse.get( "arrow_available" );
      
      if ( m_log != null ) {
        m_log.logBasic( "Python server initialized successfully:" );
        m_log.logBasic( "  PID: " + m_pythonPID );
        m_log.logBasic( "  Arrow support: " + (m_arrowAvailable ? "Available" : "Not available") );
        if ( m_useArrow && !m_arrowAvailable ) {
          m_log.logBasic( "  Note: Arrow was requested but is not available, will use CSV format" );
        }
      }

      m_shutdownHook = new Thread() {
        @Override public void run() {
          shutdown();
        }
      };
      Runtime.getRuntime().addShutdownHook( m_shutdownHook );
    }
  }

  /**
   * Launches the python server. Performs some basic requirements checks for the
   * python environment - e.g. python needs to have numpy, pandas and sklearn
   * installed.
   *
   * @param startPython true if the server is to actually be started. False is
   *                    really just for debugging/development where the server can be
   *                    manually started in a separate terminal
   * @throws IOException if a problem occurs
   */
  private void launchServer( boolean startPython ) throws IOException {
    if ( m_log != null ) {
      m_log.logBasic( "Launching Python server (startPython=" + startPython + ")" );
    }

    Thread acceptThread = startServerSocket();
    int localPort = m_serverSocket.getLocalPort();

    if ( startPython ) {
      String serverScript = s_osTmpDir + File.separator + "pyServer.py";
      boolean debug = m_log != null && m_log.isDebug();
      
      if ( m_log != null ) {
        m_log.logBasic( "Starting Python server:" );
        m_log.logBasic( "  Command: " + m_pythonCommand );
        m_log.logBasic( "  Script: " + serverScript );
        m_log.logBasic( "  Port: " + localPort );
        m_log.logBasic( "  Debug mode: " + debug );
      }
      
      ProcessBuilder
          processBuilder =
          new ProcessBuilder( m_pythonCommand, serverScript, "" + localPort, debug ? "debug" : "" );
      m_serverProcess = processBuilder.start();
      
      // Start a thread to capture error output from the Python process
      Thread errorReaderThread = new Thread() {
        @Override public void run() {
          try {
            BufferedReader errorReader = new BufferedReader(
                new InputStreamReader( m_serverProcess.getErrorStream() ) );
            String line;
            while ( ( line = errorReader.readLine() ) != null ) {
              if ( m_log != null ) {
                m_log.logError( "Python server error: " + line );
              } else {
                System.err.println( "Python server error: " + line );
              }
            }
          } catch ( IOException e ) {
            // Process ended
          }
        }
      };
      errorReaderThread.setDaemon( true );
      errorReaderThread.start();
      
      if ( m_log != null ) {
        m_log.logBasic( "Python server process started, waiting for connection..." );
      }
    }
    try {
      acceptThread.join();
    } catch ( InterruptedException e ) {
      if ( m_log != null ) {
        m_log.logError( "Interrupted while waiting for server connection" );
      }
    }

    checkLocalSocketAndCreateShutdownHook();
  }

  public void setLog( ILogChannel log ) {
    m_log = log;
  }

  /**
   * Initialize the default session. This needs to be called exactly once in order to
   * run checks and launch the server. Creates a session singleton. Assumes python
   * is in the PATH; alternatively, can specify full path to python exe via the java
   * property or system environment variable pentaho.cpython.command.
   *
   * @param pythonCommand the python command
   * @param vars          Kettle variables
   * @param log           logging
   * @return true if the server launched successfully
   * @throws HopException if there was a problem - missing packages in python,
   *                      or python could not be started for some reason
   */
  public static synchronized boolean initSession( String pythonCommand, IVariables vars, ILogChannel log )
      throws HopException {

    if ( s_sessionSingleton != null ) {
      return true;
      // throw new HopException( BaseMessages.getString( ServerUtils.PKG, "PythonSession.Error.EnvAlreadyAvailable" ) );
    }

    if ( vars != null && !org.apache.hop.core.util.Utils
        .isEmpty( vars.getVariable( HOP_CPYTHON_COMMAND_PROPERTY_KEY ) ) ) {
      pythonCommand = vars.getVariable( HOP_CPYTHON_COMMAND_PROPERTY_KEY );
      File pyExe = new File( pythonCommand );
      if ( !pyExe.exists() || !pyExe.isFile() ) {
        throw new HopException( "Python exe: " + pythonCommand + " does not seem to exist on the " + "filesystem!" );
      }
    } else if ( System.getProperty( CPYTHON_COMMAND_PROPERTY_KEY ) != null ) {
      pythonCommand = System.getProperty( CPYTHON_COMMAND_PROPERTY_KEY );
      File pyExe = new File( pythonCommand );
      if ( !pyExe.exists() || !pyExe.isFile() ) {
        throw new HopException( "Python exe: " + pythonCommand + " does not seem to exist on the " + "filesystem!" );
      }
    } else if ( System.getenv( CPYTHON_COMMAND_ENV_VAR_KEY ) != null ) {
      pythonCommand = System.getenv( CPYTHON_COMMAND_ENV_VAR_KEY );
      File pyExe = new File( pythonCommand );
      if ( !pyExe.exists() || !pyExe.isFile() ) {
        throw new HopException( "Python exe: " + pythonCommand + " does not seem to exist on the " + "filesystem!" );
      }
    }

    String osType = System.getProperty( "os.name" );
    boolean windows = osType != null && osType.toLowerCase().contains( "windows" );

    log.logDebug( "Python command: " + pythonCommand );
    String path = System.getenv( windows ? "Path" : "PATH" );
    if ( path != null && path.length() > 0 ) {
      log.logDebug( "PATH: " + path );
    }
    try {
      new PythonSession( pythonCommand, log );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }

    return s_pythonEnvCheckResults.length() < 5;
  }

  /**
   * Initialize a server/session for a user-supplied python path and (optional)
   * ownerID.
   *
   * @param pythonCommand command (either fully qualified path or that which is
   *                      in the PATH). This, plus the optional ownerID is used to lookup
   *                      and return a session/server.
   * @param ownerID       an optional ownerID string for acquiring the session. This
   *                      can be used to restrict the session/server to one (or more)
   *                      clients (i.e. those with the ID "ownerID").
   * @param pathEntries   optional entries that need to be in the PATH in order
   *                      for the python environment to work correctly
   * @param debug         true for debugging info
   * @param log           the log to use
   * @return true if the server launched successfully
   * @throws HopException if the requested session/server is not available (or
   *                      does not exist).
   */
  public static synchronized boolean initSession( String pythonCommand, String ownerID, String pathEntries,
      boolean debug, ILogChannel log ) throws HopException {
    String key = pythonCommand + ( ownerID != null && ownerID.length() > 0 ? ownerID : "" );

    if ( !m_pythonServers.containsKey( key ) ) {
      try {
        new PythonSession( pythonCommand, ownerID, pathEntries, false, log );
      } catch ( IOException ex ) {
        throw new HopException( ex );
      }
    }

    return m_pythonEnvCheckResults.get( key ).length() < 5;
  }

  /**
   * Gets the result of running the checks in python
   *
   * @return a string containing the possible errors
   */
  public static String getPythonEnvCheckResults() {
    return s_pythonEnvCheckResults;
  }

  /**
   * Gets the result of running the checks in python for the given python path +
   * optional ownerID.
   *
   * @param pythonCommand command (either fully qualified path or that which is
   *                      in the PATH). This, plus the optional ownerID is used to lookup
   *                      and return a session/server.
   * @param ownerID       an optional ownerID string for acquiring the session. This
   *                      can be used to restrict the session/server to one (or more)
   *                      clients (i.e. those with the ID "ownerID").
   * @return a string containing the possible errors
   * @throws HopException if the requested server does not exist
   */
  public static String getPythonEnvCheckResults( String pythonCommand, String ownerID ) throws HopException {
    String key = pythonCommand + ( ownerID != null && ownerID.length() > 0 ? ownerID : "" );

    if ( !m_pythonEnvCheckResults.containsKey( key ) ) {
      throw new HopException( "The specified server/environment (" + key + ") does not seem to exist!" );
    }

    return m_pythonEnvCheckResults.get( key );
  }

  /**
   * Transfer Kettle rows into python as a named pandas data frame
   *
   * @param rowMeta         the metadata of the rows
   * @param rows            the rows to transfer
   * @param pythonFrameName the name of the data frame to use in python
   * @throws HopException if a problem occurs
   */
  public void rowsToPythonDataFrame( IRowMeta rowMeta, List<Object[]> rows, String pythonFrameName )
      throws HopException {
    try {
      ServerUtils.sendRowsToPandasDataFrame( m_log, rowMeta, rows, pythonFrameName, m_localSocket.getOutputStream(),
          m_localSocket.getInputStream() );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Transfer a pandas data frame from python and convert into Kettle rows and metadata
   *
   * @param frameName       the name of the pandas data frame to get
   * @param includeRowIndex true to include the pandas data frame row index as a field
   * @return rows and row metadata encapsulated in a RowMetaAndRows object
   * @throws HopException if a problem occurs
   */
  public RowMetaAndRows rowsFromPythonDataFrame( String frameName, boolean includeRowIndex ) throws HopException {
    try {
      return ServerUtils
          .receiveRowsFromPandasDataFrame( m_log, frameName, includeRowIndex, m_localSocket.getOutputStream(),
              m_localSocket.getInputStream() );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Transfer Hop rows to a python pandas data frame using Arrow format if available
   *
   * @param rowMeta        the metadata for the rows being transferred
   * @param rows           the rows to transfer
   * @param pythonFrameName the name of the pandas data frame to create in python
   * @throws HopException if a problem occurs
   */
  public void rowsToPythonDataFrameWithArrow( IRowMeta rowMeta, List<Object[]> rows, String pythonFrameName )
      throws HopException {
    try {
      if ( m_useArrow && m_arrowAvailable ) {
        ServerUtils.sendRowsToPandasDataFrameArrow( m_log, rowMeta, rows, pythonFrameName, 
            m_localSocket.getOutputStream(), m_localSocket.getInputStream() );
      } else {
        // Fall back to CSV
        ServerUtils.sendRowsToPandasDataFrame( m_log, rowMeta, rows, pythonFrameName, 
            m_localSocket.getOutputStream(), m_localSocket.getInputStream() );
      }
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Transfer a pandas data frame from python using Arrow format if available
   *
   * @param frameName       the name of the pandas data frame to get
   * @param includeRowIndex true to include the pandas data frame row index as a field
   * @return rows and row metadata encapsulated in a RowMetaAndRows object
   * @throws HopException if a problem occurs
   */
  public RowMetaAndRows rowsFromPythonDataFrameWithArrow( String frameName, boolean includeRowIndex ) 
      throws HopException {
    try {
      if ( m_useArrow && m_arrowAvailable ) {
        return ServerUtils.receiveRowsFromPandasDataFrameArrow( m_log, frameName, includeRowIndex, 
            m_localSocket.getOutputStream(), m_localSocket.getInputStream() );
      } else {
        // Fall back to CSV
        return ServerUtils.receiveRowsFromPandasDataFrame( m_log, frameName, includeRowIndex, 
            m_localSocket.getOutputStream(), m_localSocket.getInputStream() );
      }
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Set whether to use Arrow for data transfer
   *
   * @param useArrow true to use Arrow if available
   */
  public void setUseArrow( boolean useArrow ) {
    m_useArrow = useArrow;
  }

  /**
   * Check if Arrow support is available
   *
   * @return true if Arrow is available in Python
   */
  public boolean isArrowAvailable() {
    return m_arrowAvailable;
  }

  /**
   * Check if a named variable is set in pythong
   *
   * @param pyVarName the name of the python variable to check for
   * @return true if the named variable exists in the python environment
   * @throws HopException if a problem occurs
   */
  public boolean checkIfPythonVariableIsSet( String pyVarName ) throws HopException {
    try {
      return ServerUtils.checkIfPythonVariableIsSet( m_log, pyVarName, m_localSocket.getInputStream(),
          m_localSocket.getOutputStream() );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Grab the contents of the debug buffer from the python server. The server
   * redirects both sys out and sys err to StringIO objects. If debug has been
   * specified, then server debugging output will have been collected in these
   * buffers. Note that the buffers will potentially also contain output from
   * the execution of arbitrary scripts too. Calling this method also resets the
   * buffers.
   *
   * @return the contents of the sys out and sys err streams. Element 0 in the
   * list contains sys out and element 1 contains sys err
   * @throws HopException if a problem occurs
   */
  public List<String> getPythonDebugBuffer() throws HopException {
    try {
      return ServerUtils.receiveDebugBuffer( m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Get the type of a variable (according to pre-defined types that we can do something with) in python
   *
   * @param varName the name of the variable to get the type of
   * @return whether the variable is a DataFrame, Image, String or Unknown. DataFrames will be converted to rows; Images
   * (png form) and Strings are output as a single row with fields for each variable value. Unknown types will be
   * returned in their string form.
   * @throws HopException if a problem occurs
   */
  public PythonVariableType getPythonVariableType( String varName ) throws HopException {
    try {
      return ServerUtils
          .getPythonVariableType( varName, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Execute a python script.
   *
   * @param pyScript the script to execute
   * @return a List of strings - index 0 contains std out from the script and
   * index 1 contains std err
   * @throws HopException if a problem occurs
   */
  public List<String> executeScript( String pyScript ) throws HopException {
    try {
      return ServerUtils
          .executeUserScript( pyScript, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Get an image from python. Assumes that the image is a matplotlib.figure.Figure object. Retrieves this as png
   * data and returns a BufferedImage
   *
   * @param varName the name of the variable containing the image in python
   * @return the image as a BufferedImage
   * @throws HopException if a problem occurs
   */
  public BufferedImage getImageFromPython( String varName ) throws HopException {
    try {
      return ServerUtils
          .getPNGImageFromPython( varName, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Get the value of a variable from python in plain text form
   *
   * @param varName the name of the variable to get
   * @return the value of the variable
   * @throws HopException if a problem occurs
   */
  public String getVariableValueFromPythonAsPlainString( String varName ) throws HopException {
    try {
      return ServerUtils
          .receivePickledVariableValue( varName, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), true,
              m_log );
    } catch ( IOException ex ) {
      throw new HopException( ex );
    }
  }

  /**
   * Shutdown the python server
   */
  private void shutdown() {
    if ( !m_shutdown ) {
      try {
        m_shutdown = true;
        if ( m_localSocket != null ) {
          if ( m_log == null ) {
            System.err.println( "Sending shutdown command..." );
          } else if ( m_log.isDebug() ) {
            m_log.logDebug( "Sending shutdown command..." );
          }
          if ( m_log == null || m_log.isDebug() ) {
            List<String>
                outAndErr =
                ServerUtils
                    .receiveDebugBuffer( m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
            if ( outAndErr.get( 0 ).length() > 0 ) {
              if ( m_log == null ) {
                System.err.println( "Python debug std out:\n" + outAndErr.get( 0 ) + "\n" );
              } else {
                m_log.logDebug( "Python debug std out:\n" + outAndErr.get( 0 ) + "\n" );
              }
            }
            if ( outAndErr.get( 1 ).length() > 0 ) {
              if ( m_log == null ) {
                System.err.println( "Python debug std err:\n" + outAndErr.get( 1 ) + "\n" );
              } else {
                m_log.logDebug( "Python debug std err:\n" + outAndErr.get( 1 ) + "\n" );
              }
            }
          }
          ServerUtils.sendServerShutdown( m_localSocket.getOutputStream() );
          m_localSocket.close();
          if ( m_serverProcess != null ) {
            m_serverProcess.destroy();
            m_serverProcess = null;
          }
        }

        if ( m_serverSocket != null ) {
          m_serverSocket.close();
        }
        s_sessionSingleton = null;
      } catch ( Exception ex ) {
        ex.printStackTrace();
        if ( m_pythonPID > 0 ) {
          // try to kill process, just in case
          ProcessBuilder killer;
          if ( System.getProperty( "os.name" ).toLowerCase().contains( "win" ) ) {
            killer = new ProcessBuilder( "taskkill", "/F", "/PID", "" + m_pythonPID );
          } else {
            killer = new ProcessBuilder( "kill", "-9", "" + m_pythonPID );
          }
          try {
            killer.start();
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        }
      } finally {
        if ( m_serverProcess != null ) {
          m_serverProcess.destroy();
        }
      }
    }
  }

  public static void main( String[] args ) {
    try {
      if ( !PythonSession.initSession( "python", null, null ) ) {
        System.err.print( "Initialization failed!" );
        System.exit( 1 );
      }

      String temp = "";
      PythonSession session = PythonSession.acquireSession( temp );

      Object[] rowData = { 22, 300.22, "Hello bob", false, new Date(), new Timestamp( new Date().getTime() ), null };
      IRowMeta rowMeta = new RowMeta();
      //rowMeta.addValueMeta( new ValueMeta( "Field1", IValueMeta.TYPE_INTEGER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field1", IValueMeta.TYPE_INTEGER ) );
      //rowMeta.addValueMeta( new ValueMeta( "Field2", IValueMeta.TYPE_NUMBER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field2", IValueMeta.TYPE_NUMBER ) );
      // rowMeta.addValueMeta( new ValueMeta( "Field3", IValueMeta.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field3", IValueMeta.TYPE_STRING ) );
      // rowMeta.addValueMeta( new ValueMeta( "Field4", IValueMeta.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field4", IValueMeta.TYPE_BOOLEAN ) );
      // rowMeta.addValueMeta( new ValueMeta( "Field5", IValueMeta.TYPE_DATE ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field5", IValueMeta.TYPE_DATE ) );
      // rowMeta.addValueMeta( new ValueMeta( "Field6", IValueMeta.TYPE_TIMESTAMP ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field6", IValueMeta.TYPE_TIMESTAMP ) );
      // rowMeta.addValueMeta( new ValueMeta( "NullField", IValueMeta.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "NullField", IValueMeta.TYPE_STRING ) );

      List<Object[]> data = new ArrayList<Object[]>();
      data.add( rowData );
      session.rowsToPythonDataFrame( rowMeta, data, "test" );

      if ( session.checkIfPythonVariableIsSet( "test" ) ) {
        RowMetaAndRows fromPy = session.rowsFromPythonDataFrame( "test", false );
        System.err.println( "Nubmer of field metas returned: " + fromPy.m_rowMeta.size() );
        System.err.println( "Number of rows returned: " + fromPy.m_rows.length );
        for ( IValueMeta v : fromPy.m_rowMeta.getValueMetaList() ) {
          System.err.println( "Col: " + v.getName() + " Type: " + v.getType() );
        }
      } else {
        System.err.println( "Variable 'test' does not seem to be set in python!!!!" );
      }

      session.executeScript( "x = 100\n" );
      if ( session.checkIfPythonVariableIsSet( "x" ) ) {
        System.err.println( "Var x is set!" );
      }
      PythonVariableType t = session.getPythonVariableType( "x" );
      System.err.println( "X is of type " + t.toString() );
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
  }
}
