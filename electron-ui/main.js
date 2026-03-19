const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');

let mainWindow;
let activeAgentRun = null;
let isAppShuttingDown = false;

// ---------------------------------------------------------------------------
// Python backend configuration  (works in both dev and packaged modes)
// ---------------------------------------------------------------------------
function getPythonConfig() {
  if (app.isPackaged) {
    // Packaged mode – use the PyInstaller-bundled dispatcher executable
    const backendDir = path.join(process.resourcesPath, 'python-backend');
    return {
      packaged: true,
      backendDir,
      dispatcherExe: path.join(backendDir, 'dispatcher.exe'),
      scriptsDir: path.join(backendDir, 'scripts'),
      // Working directory for spawned processes – use the directory that
      // contains the .exe so the user can place a .env file next to it.
      cwd: path.dirname(process.execPath)
    };
  }
  // Development mode – use system Python
  return {
    packaged: false,
    backendDir: null,
    dispatcherExe: null,
    scriptsDir: path.join(__dirname, '..', 'scripts'),
    pythonCmd: process.platform === 'win32' ? 'python' : 'python3',
    cwd: path.join(__dirname, '..')
  };
}

// Initialised once app is ready (see createWindow); declared here for module scope.
let pyConfig;

/**
 * Spawn a Python script.
 *
 * In **dev mode** this calls ``python -u scripts/<name> ...``
 * In **packaged mode** this calls ``dispatcher.exe <name> ...``
 *
 * Returns the child_process.ChildProcess handle.
 */
function spawnPythonProcess(scriptName, args = []) {
  const env = {
    ...process.env,
    PYTHONIOENCODING: 'utf-8',
    PYTHONUNBUFFERED: '1'
  };

  if (pyConfig.packaged) {
    // Tell the dispatcher where the user's .env lives
    const dotenvPath = path.join(pyConfig.cwd, '.env');
    if (fs.existsSync(dotenvPath)) {
      env.DOTENV_PATH = dotenvPath;
    }
    return spawn(pyConfig.dispatcherExe, [scriptName, ...args], {
      cwd: pyConfig.cwd,
      env
    });
  }
  // Dev mode – invoke system Python directly
  const scriptPath = path.join(pyConfig.scriptsDir, scriptName);
  return spawn(pyConfig.pythonCmd, ['-u', scriptPath, ...args], {
    cwd: pyConfig.cwd,
    env
  });
}

function createWindow() {
  pyConfig = getPythonConfig();

  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 900,
    minHeight: 600,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    },
    titleBarStyle: 'hiddenInset',
    backgroundColor: '#0a0a0f'
  });

  mainWindow.loadFile('index.html');

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
  
  // Open DevTools in development
  if (process.argv.includes('--dev')) {
    mainWindow.webContents.openDevTools();
  }
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('before-quit', () => {
  isAppShuttingDown = true;
  console.info('[Shutdown] before-quit triggered');
  if (activeAgentRun && activeAgentRun.process && !activeAgentRun.process.killed) {
    try {
      console.info(`[Shutdown] Stopping active agent run ${activeAgentRun.runId}`);
      activeAgentRun.process.kill('SIGTERM');
    } catch (err) {
      // Ignore termination errors during shutdown.
      console.warn('[Shutdown] Failed to terminate active agent process:', err.message);
    }
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});

// Helper to run Python scripts with optional streaming
function runPythonScript(scriptName, args = [], options = {}) {
  const { streaming = false, streamId = null, target = 'default' } = options;
  
  return new Promise((resolve, reject) => {
    const pythonProcess = spawnPythonProcess(scriptName, args);

    let stdout = '';
    let stderr = '';

    pythonProcess.stdout.on('data', (data) => {
      const text = data.toString();
      stdout += text;
      
      // Send streaming output to renderer if enabled
      if (streaming) {
        // Parse and emit tool calls separately
        const lines = text.split('\n');
        for (const line of lines) {
          if (line.startsWith('[TOOL]')) {
            sendToRenderer('tool-call', {
              streamId,
              target,
              tool: line.replace('[TOOL]', '').trim()
            }, 'runPythonScript stdout tool');
          } else if (line.startsWith('[DEBUG]')) {
            sendToRenderer('stream-output', {
              streamId,
              target,
              text: line,
              type: 'debug'
            }, 'runPythonScript stdout debug');
          } else if (line.trim()) {
            sendToRenderer('stream-output', {
              streamId,
              target,
              text: line,
              type: 'output'
            }, 'runPythonScript stdout output');
          }
        }
      }
    });

    pythonProcess.stderr.on('data', (data) => {
      const text = data.toString();
      stderr += text;
      
      // Stream stderr too (useful for verbose output)
      if (streaming) {
        sendToRenderer('stream-output', {
          streamId,
          target,
          text,
          type: 'stderr'
        }, 'runPythonScript stderr');
      }
    });

    pythonProcess.on('close', (code) => {
      if (streaming) {
        sendToRenderer('stream-complete', {
          streamId,
          target,
          success: code === 0
        }, 'runPythonScript close');
      }
      
      if (code === 0) {
        resolve(stdout);
      } else {
        reject(new Error(stderr || `Process exited with code ${code}`));
      }
    });

    pythonProcess.on('error', (err) => {
      reject(err);
    });
  });
}

function runPythonScriptWithStdin(scriptName, args = [], payload = null, options = {}) {
  const { streaming = false, streamId = null, target = 'default' } = options;
  return new Promise((resolve, reject) => {
    const pythonProcess = spawnPythonProcess(scriptName, args);
    let stdout = '';
    let stderr = '';

    pythonProcess.stdout.on('data', (data) => {
      const text = data.toString();
      stdout += text;
      if (streaming) {
        const lines = text.split('\n');
        for (const line of lines) {
          if (line.startsWith('[TOOL]')) {
            sendToRenderer('tool-call', {
              streamId,
              target,
              tool: line.replace('[TOOL]', '').trim()
            }, 'runPythonScriptWithStdin stdout tool');
          } else if (line.startsWith('[DEBUG]')) {
            sendToRenderer('stream-output', {
              streamId,
              target,
              text: line,
              type: 'debug'
            }, 'runPythonScriptWithStdin stdout debug');
          } else if (line.trim()) {
            sendToRenderer('stream-output', {
              streamId,
              target,
              text: line,
              type: 'output'
            }, 'runPythonScriptWithStdin stdout output');
          }
        }
      }
    });

    pythonProcess.stderr.on('data', (data) => {
      const text = data.toString();
      stderr += text;
      if (streaming) {
        sendToRenderer('stream-output', {
          streamId,
          target,
          text,
          type: 'stderr'
        }, 'runPythonScriptWithStdin stderr');
      }
    });

    pythonProcess.on('close', (code) => {
      if (streaming) {
        sendToRenderer('stream-complete', {
          streamId,
          target,
          success: code === 0
        }, 'runPythonScriptWithStdin close');
      }
      if (code === 0) {
        resolve(stdout);
      } else {
        reject(new Error(stderr || `Process exited with code ${code}`));
      }
    });

    pythonProcess.on('error', (err) => {
      reject(err);
    });

    if (payload !== null && pythonProcess.stdin) {
      pythonProcess.stdin.write(typeof payload === 'string' ? payload : JSON.stringify(payload));
    }
    if (pythonProcess.stdin) {
      pythonProcess.stdin.end();
    }
  });
}

function parseJsonFromMixedOutput(output, fallback = {}) {
  const text = String(output || '').trim();
  if (!text) return fallback;
  try {
    return JSON.parse(text);
  } catch {
    const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      const line = lines[i];
      if (!line.startsWith('{') && !line.startsWith('[')) continue;
      try {
        return JSON.parse(line);
      } catch {
        // Continue scanning upward.
      }
    }
  }
  return fallback;
}

function normalizeAgentConfig(config = {}) {
  const thresholds = (config && typeof config.thresholds === 'object' && config.thresholds) || {};
  const scope = (config && typeof config.scope === 'object' && config.scope) || {};
  return {
    pollIntervalMs: Math.max(1000, Number(config.pollIntervalMs || 1000)),
    historyWindowMinutes: Math.max(10, Number(config.historyWindowMinutes || 360)),
    minHistoryPoints: Math.max(10, Number(config.minHistoryPoints || 30)),
    maxMonitoredTags: Math.max(10, Number(config.maxMonitoredTags || 200)),
    maxCandidatesPerCycle: Math.max(1, Number(config.maxCandidatesPerCycle || 25)),
    maxCandidatesPerSubsystem: Math.max(1, Number(config.maxCandidatesPerSubsystem || 8)),
    maxLlmTriagesPerCycle: Math.max(0, Number(config.maxLlmTriagesPerCycle ?? 5)),
    maxLlmTriagesPerSubsystem: Math.max(0, Number(config.maxLlmTriagesPerSubsystem ?? 2)),
    dedupCooldownMinutes: Math.max(1, Number(config.dedupCooldownMinutes || 10)),
    retentionDays: Math.max(1, Number(config.retentionDays || 14)),
    cleanupEveryCycles: Math.max(1, Number(config.cleanupEveryCycles || 40)),
    thresholds: {
      z: Number(thresholds.z ?? 3.0),
      mad: Number(thresholds.mad ?? 3.5),
      rate: Number(thresholds.rate ?? 0.0),
      stalenessSec: Number(thresholds.stalenessSec ?? 120),
      flatline_std_epsilon: Number(thresholds.flatline_std_epsilon ?? 1e-6),
      stuck_window_size: Number(thresholds.stuck_window_size ?? 20),
    },
    scope: {
      project: scope.project || null,
      equipmentTags: Array.isArray(scope.equipmentTags) ? scope.equipmentTags : [],
      tagRegex: scope.tagRegex || null,
      subsystemMode: String(scope.subsystemMode || 'auto').toLowerCase() === 'global' ? 'global' : 'auto',
      subsystemPriority: Array.isArray(scope.subsystemPriority) && scope.subsystemPriority.length
        ? scope.subsystemPriority.map(String)
        : ['view', 'equipment', 'group', 'global'],
      subsystemInclude: Array.isArray(scope.subsystemInclude) ? scope.subsystemInclude.map(String) : [],
      includeUnlinkedTags: Boolean(scope.includeUnlinkedTags),
    },
  };
}

function canSendToRenderer() {
  if (!mainWindow) return false;
  if (typeof mainWindow.isDestroyed === 'function' && mainWindow.isDestroyed()) return false;
  const wc = mainWindow.webContents;
  if (!wc) return false;
  if (typeof wc.isDestroyed === 'function' && wc.isDestroyed()) return false;
  return true;
}

function sendToRenderer(channel, payload, context = '') {
  if (!canSendToRenderer()) {
    if (isAppShuttingDown) {
      console.info(`[Shutdown] Dropped renderer message ${channel}${context ? ` (${context})` : ''}`);
    } else {
      console.warn(`[IPC] Renderer unavailable for ${channel}${context ? ` (${context})` : ''}`);
    }
    return false;
  }
  try {
    mainWindow.webContents.send(channel, payload);
    return true;
  } catch (err) {
    console.warn(`[IPC] Failed sending ${channel}${context ? ` (${context})` : ''}: ${err.message}`);
    return false;
  }
}

function routeAgentMessage(channel, payload) {
  const ok = sendToRenderer(channel, payload, 'agent-stream');
  if (!ok) {
    console.warn(`[Agent IPC] Failed to route message on ${channel}`);
  }
}

function parseAgentLine(line) {
  const trimmed = (line || '').trim();
  if (!trimmed) return null;
  const prefixes = [
    { key: '[AGENT_STATUS]', channel: 'agent-status' },
    { key: '[AGENT_EVENT]', channel: 'agent-event' },
    { key: '[AGENT_ERROR]', channel: 'agent-error' },
    { key: '[AGENT_COMPLETE]', channel: 'agent-complete' },
  ];
  for (const prefix of prefixes) {
    if (!trimmed.startsWith(prefix.key)) continue;
    const jsonText = trimmed.slice(prefix.key.length).trim();
    try {
      const payload = JSON.parse(jsonText);
      return { channel: prefix.channel, payload };
    } catch (err) {
      return {
        channel: 'agent-error',
        payload: {
          runId: activeAgentRun ? activeAgentRun.runId : null,
          code: 'invalid_agent_json',
          message: `Failed to parse agent stream line: ${trimmed.slice(0, 200)}`,
          recoverable: true,
          timestamp: new Date().toISOString(),
        },
      };
    }
  }
  return null;
}

function handleAgentStdoutChunk(text) {
  if (!activeAgentRun) return;
  activeAgentRun.stdoutBuffer += text;
  const lines = activeAgentRun.stdoutBuffer.split(/\r?\n/);
  activeAgentRun.stdoutBuffer = lines.pop() || '';
  for (const line of lines) {
    const parsed = parseAgentLine(line);
    if (!parsed) {
      if (line.trim().startsWith('[AGENT')) {
        console.warn('[Agent stream] Unparsed line:', line.slice(0, 300));
      }
      continue;
    }
    if (parsed.channel === 'agent-status' && parsed.payload) {
      activeAgentRun.status = parsed.payload.state || activeAgentRun.status;
      activeAgentRun.metrics = {
        cycleMs: parsed.payload.cycleMs || 0,
        candidates: parsed.payload.candidates || 0,
        triaged: parsed.payload.triaged || 0,
        emitted: parsed.payload.emitted || 0,
        timestamp: parsed.payload.timestamp || new Date().toISOString(),
      };
    }
    routeAgentMessage(parsed.channel, parsed.payload);
  }
}

async function stopActiveAgent(reason = 'stopped_by_user') {
  if (!activeAgentRun || !activeAgentRun.process || activeAgentRun.process.killed) {
    return { success: false, error: 'No active agent run' };
  }
  const runId = activeAgentRun.runId;
  activeAgentRun.status = 'stopping';

  return new Promise((resolve) => {
    const proc = activeAgentRun.process;
    let settled = false;
    const done = (result) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };

    proc.once('close', () => {
      done({ success: true, runId, stoppedAt: new Date().toISOString(), reason });
    });

    try {
      proc.kill('SIGTERM');
    } catch (err) {
      done({ success: false, error: err.message });
      return;
    }

    setTimeout(() => {
      if (proc.killed) return;
      try {
        proc.kill('SIGKILL');
      } catch (err) {
        // Ignore forced termination errors.
      }
    }, 5000);
  });
}

// IPC Handlers

// Select file dialog
ipcMain.handle('select-file', async (event, options) => {
  const properties = ['openFile'];
  if (options && options.multiple) properties.push('multiSelections');
  const result = await dialog.showOpenDialog(mainWindow, {
    properties,
    filters: (options && options.filters) || [
      { name: 'All Supported', extensions: ['json', 'sc', 'L5X', 'st', 'xml'] },
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'Rockwell PLC', extensions: ['sc', 'L5X'] },
      { name: 'Siemens PLC', extensions: ['st'] },
      { name: 'TIA Portal XML', extensions: ['xml'] }
    ]
  });
  if (options && options.multiple) {
    return { filePaths: result.filePaths || [] };
  }
  return result.filePaths[0] || null;
});

// Select directory dialog
ipcMain.handle('select-directory', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openDirectory']
  });
  return result.filePaths[0] || null;
});

// Ingest PLC files - Rockwell (with streaming)
ipcMain.handle('ingest-plc', async (event, filePath) => {
  const streamId = `ingest-plc-${Date.now()}`;
  try {
    const output = await runPythonScript('ontology_analyzer.py', [filePath, '-v'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Ingest Siemens ST files (with streaming)
ipcMain.handle('ingest-siemens', async (event, filePath) => {
  const streamId = `ingest-siemens-${Date.now()}`;
  try {
    const output = await runPythonScript('ontology_analyzer.py', [filePath, '--siemens', '-v'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Ingest Siemens TIA Portal XML exports (with streaming)
ipcMain.handle('ingest-tia-xml', async (event, filePath) => {
  const streamId = `ingest-tia-xml-${Date.now()}`;
  try {
    const output = await runPythonScript('ontology_analyzer.py', [filePath, '--tia-xml', '-v', '--skip-ai'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Ingest entire Siemens TIA Portal project (with streaming)
ipcMain.handle('ingest-tia-project', async (event, folderPath) => {
  const streamId = `ingest-tia-project-${Date.now()}`;
  try {
    const output = await runPythonScript('ontology_analyzer.py', [folderPath, '--tia-project', '-v', '--skip-ai'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Ingest Ignition files (with streaming)
// Uses --skip-ai to avoid token limits on large multi-project gateways
// Use incremental_analyzer.py to enrich items later
ipcMain.handle('ingest-ignition', async (event, options) => {
  const { filePath, scriptLibraryPath, namedQueriesPath } = options;
  const streamId = `ingest-ignition-${Date.now()}`;
  try {
    // Build args with optional directory paths
    const args = [filePath, '-v', '--skip-ai'];
    if (scriptLibraryPath) {
      args.push('--script-library', scriptLibraryPath);
    }
    if (namedQueriesPath) {
      args.push('--named-queries', namedQueriesPath);
    }
    
    // Always skip AI during initial ingestion - use incremental analyzer for enrichment
    const output = await runPythonScript('ignition_ontology.py', args, {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId, filePath };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Ingest Workbench backup (project.json format from Axilon Workbench)
ipcMain.handle('ingest-workbench', async (event, folderPath) => {
  const streamId = `ingest-workbench-${Date.now()}`;
  try {
    // Workbench parser takes the project.json path (or folder containing it)
    let projectJsonPath = folderPath;
    if (!folderPath.endsWith('.json')) {
      projectJsonPath = path.join(folderPath, 'project.json');
    }
    
    const output = await runPythonScript('workbench_ingest.py', [projectJsonPath, '-v'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId, folderPath };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Run unified analysis (with streaming)
ipcMain.handle('run-unified', async () => {
  const streamId = `unified-${Date.now()}`;
  try {
    const output = await runPythonScript('unified_ontology.py', ['--analyze', '-v'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Run troubleshooting enrichment (with streaming)
ipcMain.handle('run-enrichment', async () => {
  const streamId = `enrichment-${Date.now()}`;
  try {
    const output = await runPythonScript('troubleshooting_ontology.py', ['--enrich-all', '-v'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Clear database
ipcMain.handle('clear-database', async () => {
  try {
    // Use neo4j_ontology.py with 'yes' piped to stdin
    return new Promise((resolve, reject) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['clear']);
      
      // Send 'yes' to confirm
      proc.stdin.write('yes\n');
      proc.stdin.end();
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          resolve({ success: true, output: stdout });
        } else {
          resolve({ success: false, error: stderr || 'Failed to clear database' });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Initialize database schema
ipcMain.handle('init-database', async () => {
  try {
    const output = await runPythonScript('neo4j_ontology.py', ['init']);
    return { success: true, output };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Get database stats
ipcMain.handle('get-stats', async () => {
  try {
    const output = await runPythonScript('claude_client.py', ['--schema']);
    return { success: true, output };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Troubleshooting query with conversation history (with streaming tool calls)
ipcMain.handle('troubleshoot', async (event, question, history) => {
  const streamId = `troubleshoot-${Date.now()}`;
  
  try {
    // Prepare JSON payload with question and history
    const payload = JSON.stringify({
      question: question,
      history: history || []
    });
    
    return new Promise((resolve, reject) => {
      const proc = spawnPythonProcess('troubleshoot.py', ['--history', '-v']);
      
      // Send JSON to stdin
      proc.stdin.write(payload);
      proc.stdin.end();
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { 
        stdout += data.toString(); 
      });
      
      proc.stderr.on('data', (data) => { 
        const text = data.toString();
        stderr += text;
        
        // Stream tool calls, debug info, and Claude response from stderr to frontend
        if (canSendToRenderer()) {
          // Check for special prefixes first (they appear on their own lines)
          if (text.includes('[TOOL]') || text.includes('[DEBUG]') || text.includes('[INFO]')) {
            const lines = text.split('\n');
            for (const line of lines) {
              if (line.startsWith('[TOOL]')) {
                sendToRenderer('tool-call', {
                  streamId,
                  tool: line.replace('[TOOL]', '').trim()
                }, 'troubleshoot stderr tool');
              } else if (line.startsWith('[DEBUG]') || line.startsWith('[INFO]')) {
                sendToRenderer('stream-output', {
                  streamId,
                  text: line,
                  type: 'debug'
                }, 'troubleshoot stderr debug');
              }
            }
          } else if (text.includes('[STREAM]')) {
            // Start of Claude streaming - send what comes after [STREAM]
            const streamStart = text.indexOf('[STREAM]');
            const afterStream = text.substring(streamStart + 8); // 8 = length of '[STREAM]'
            if (afterStream) {
              sendToRenderer('stream-output', {
                streamId,
                text: afterStream,
                type: 'claude-stream'
              }, 'troubleshoot stderr stream-start');
            }
          } else if (text && !text.startsWith('[')) {
            // Continuation of Claude streaming (no prefix)
            sendToRenderer('stream-output', {
              streamId,
              text: text,
              type: 'claude-stream'
            }, 'troubleshoot stderr stream-cont');
          }
        }
      });
      
      proc.on('close', (code) => {
        if (canSendToRenderer()) {
          sendToRenderer('stream-complete', {
            streamId,
            success: code === 0
          }, 'troubleshoot close');
        }
        
        if (code === 0) {
          try {
            const result = JSON.parse(stdout);
            resolve({ 
              success: true, 
              response: result.response,
              history: result.history,
              streamId
            });
          } catch (e) {
            // Fallback if JSON parsing fails
            resolve({ success: true, response: stdout, history: [], streamId });
          }
        } else {
          // Filter out tool call lines from error message
          const cleanError = stderr
            .split('\n')
            .filter(line => !line.startsWith('[TOOL]') && !line.startsWith('[DEBUG]'))
            .join('\n')
            .trim();
          resolve({ success: false, error: cleanError || 'Query failed', streamId });
        }
      });
      
      proc.on('error', (err) => {
        resolve({ success: false, error: err.message, streamId });
      });
    });
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

ipcMain.handle('cases:assistant-query', async (event, question, history, context, options = {}) => {
  const streamId = options.streamId || `cases-assistant-${Date.now()}`;
  const target = options.target || 'cases-assistant';

  try {
    const payload = JSON.stringify({
      question: question,
      history: history || [],
      context: context || '',
    });

    return new Promise((resolve) => {
      const proc = spawnPythonProcess('troubleshoot.py', ['--history', '-v']);

      proc.stdin.write(payload);
      proc.stdin.end();

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      proc.stderr.on('data', (data) => {
        const text = data.toString();
        stderr += text;

        if (canSendToRenderer()) {
          if (text.includes('[TOOL]') || text.includes('[DEBUG]') || text.includes('[INFO]')) {
            const lines = text.split('\n');
            for (const line of lines) {
              if (line.startsWith('[TOOL]')) {
                sendToRenderer('tool-call', {
                  streamId,
                  target,
                  tool: line.replace('[TOOL]', '').trim()
                }, 'cases assistant stderr tool');
              } else if (line.startsWith('[DEBUG]') || line.startsWith('[INFO]')) {
                sendToRenderer('stream-output', {
                  streamId,
                  target,
                  text: line,
                  type: 'debug'
                }, 'cases assistant stderr debug');
              }
            }
          } else if (text.includes('[STREAM]')) {
            const streamStart = text.indexOf('[STREAM]');
            const afterStream = text.substring(streamStart + 8);
            if (afterStream) {
              sendToRenderer('stream-output', {
                streamId,
                target,
                text: afterStream,
                type: 'claude-stream'
              }, 'cases assistant stderr stream-start');
            }
          } else if (text && !text.startsWith('[')) {
            sendToRenderer('stream-output', {
              streamId,
              target,
              text: text,
              type: 'claude-stream'
            }, 'cases assistant stderr stream-cont');
          }
        }
      });

      proc.on('close', (code) => {
        if (canSendToRenderer()) {
          sendToRenderer('stream-complete', {
            streamId,
            target,
            success: code === 0
          }, 'cases assistant close');
        }

        if (code === 0) {
          try {
            const result = JSON.parse(stdout);
            resolve({
              success: true,
              response: result.response,
              history: result.history,
              streamId
            });
          } catch (e) {
            resolve({ success: true, response: stdout, history: [], streamId });
          }
        } else {
          const cleanError = stderr
            .split('\n')
            .filter(line => !line.startsWith('[TOOL]') && !line.startsWith('[DEBUG]'))
            .join('\n')
            .trim();
          resolve({ success: false, error: cleanError || 'Case investigator query failed', streamId });
        }
      });

      proc.on('error', (err) => {
        resolve({ success: false, error: err.message, streamId });
      });
    });
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Generate visualization
ipcMain.handle('generate-viz', async () => {
  try {
    // In packaged mode write to a temp-friendly location next to the exe
    const outputPath = app.isPackaged
      ? path.join(path.dirname(process.execPath), 'ontology_graph.html')
      : path.join(__dirname, '..', 'ontology_graph.html');
    await runPythonScript('ontology_viewer.py', ['-o', outputPath]);
    return { success: true, path: outputPath };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Save database to file
ipcMain.handle('save-database', async () => {
  try {
    // Show save dialog
    const result = await dialog.showSaveDialog(mainWindow, {
      title: 'Save Database Backup',
      defaultPath: `neo4j_backup_${new Date().toISOString().split('T')[0]}.json`,
      filters: [
        { name: 'JSON Files', extensions: ['json'] },
        { name: 'All Files', extensions: ['*'] }
      ]
    });
    
    if (result.canceled || !result.filePath) {
      return { success: false, error: 'Save cancelled' };
    }
    
    const output = await runPythonScript('neo4j_ontology.py', ['export', '-f', result.filePath]);
    return { success: true, path: result.filePath, output };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Load database from file
ipcMain.handle('load-database', async () => {
  try {
    // Show open dialog
    const result = await dialog.showOpenDialog(mainWindow, {
      title: 'Load Database Backup',
      filters: [
        { name: 'JSON Files', extensions: ['json'] },
        { name: 'All Files', extensions: ['*'] }
      ],
      properties: ['openFile']
    });
    
    if (result.canceled || !result.filePaths[0]) {
      return { success: false, error: 'Load cancelled' };
    }
    
    const filePath = result.filePaths[0];
    
    // Run load with --yes to skip confirmation (we'll confirm in UI)
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['load', '-f', filePath, '--yes']);
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          resolve({ success: true, path: filePath, output: stdout });
        } else {
          resolve({ success: false, error: stderr || stdout || 'Load failed' });
        }
      });
      
      proc.on('error', (err) => {
        resolve({ success: false, error: err.message });
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// ============================================
// Project and Browse Tab IPC Handlers
// ============================================

// Get all projects with inheritance info
ipcMain.handle('get-projects', async () => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['projects', '--json']);
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          try {
            const projects = JSON.parse(stdout.trim() || '[]');
            resolve({ success: true, projects });
          } catch (e) {
            resolve({ success: true, projects: [] });
          }
        } else {
          resolve({ success: false, error: stderr || 'Failed to get projects', projects: [] });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message, projects: [] };
  }
});

// Get gateway-wide resources (Tags, UDTs, AOIs)
ipcMain.handle('get-gateway-resources', async () => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['gateway-resources', '--json']);
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          try {
            const resources = JSON.parse(stdout.trim() || '{}');
            resolve({ success: true, resources });
          } catch (e) {
            resolve({ success: true, resources: { tags: [], udts: [], aois: [] } });
          }
        } else {
          resolve({ success: false, error: stderr || 'Failed to get gateway resources' });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Get project-specific resources (Views, Scripts, Queries, Events)
ipcMain.handle('get-project-resources', async (event, projectName) => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['project-resources', '--project', projectName, '--json']);
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          try {
            const resources = JSON.parse(stdout.trim() || '{}');
            resolve({ success: true, resources });
          } catch (e) {
            resolve({ success: true, resources: { views: [], scripts: [], queries: [], events: [] } });
          }
        } else {
          resolve({ success: false, error: stderr || 'Failed to get project resources' });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Get all TIA Portal projects
ipcMain.handle('get-tia-projects', async () => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['tia-projects', '--json']);
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          try {
            const projects = JSON.parse(stdout.trim() || '[]');
            resolve({ success: true, projects });
          } catch (e) {
            resolve({ success: true, projects: [] });
          }
        } else {
          resolve({ success: false, error: stderr || 'Failed to get TIA projects', projects: [] });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message, projects: [] };
  }
});

// Get TIA project-specific resources (PLC blocks, tags, types, HMI scripts, alarms, screens, etc.)
ipcMain.handle('get-tia-project-resources', async (event, projectName) => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['tia-project-resources', '--project', projectName, '--json']);
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          try {
            const resources = JSON.parse(stdout.trim() || '{}');
            resolve({ success: true, resources });
          } catch (e) {
            resolve({ success: true, resources: {} });
          }
        } else {
          resolve({ success: false, error: stderr || 'Failed to get TIA project resources' });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Enrich Siemens TIA items (no backup file needed -- context from Neo4j)
ipcMain.handle('enrich-tia-batch', async (event, options = {}) => {
  const { itemType, batchSize = 10 } = options;
  const streamId = `enrich-tia-${itemType}-${Date.now()}`;
  
  try {
    if (!itemType) {
      return { success: false, error: 'Item type required for TIA enrichment' };
    }
    
    const args = ['analyze', '-t', itemType, '-b', String(batchSize), '-v'];
    
    const output = await runPythonScript('incremental_analyzer.py', args, {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// Get enrichment status (optionally filtered by project or item type)
ipcMain.handle('get-enrichment-status', async (event, options = {}) => {
  try {
    const { project, itemType } = options;
    const args = ['status', '--json'];
    if (project) args.push('--project', project);
    if (itemType) args.push('--type', itemType);
    
    const output = await runPythonScript('incremental_analyzer.py', args);
    try {
      const status = JSON.parse(output.trim() || '{}');
      return { success: true, status };
    } catch (e) {
      return { success: true, status: {} };
    }
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Run enrichment batch for specific project and/or item type
ipcMain.handle('enrich-batch', async (event, options = {}) => {
  const { project, itemType, batchSize = 10, inputFile } = options;
  const streamId = `enrich-${project || 'gateway'}-${Date.now()}`;
  
  try {
    if (!inputFile) {
      return { success: false, error: 'Input file required for enrichment' };
    }
    
    const args = ['analyze', '-i', inputFile, '-b', String(batchSize), '-v'];
    if (project) args.push('--project', project);
    if (itemType) args.push('--type', itemType);
    
    const output = await runPythonScript('incremental_analyzer.py', args, {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// ============================================
// Graph API IPC Handlers
// ============================================

// Load graph data
ipcMain.handle('graph:load', async (event, options = {}) => {
  try {
    const args = ['load'];
    if (options.types && options.types.length > 0) {
      args.push('--types', ...options.types);
    }
    if (options.limit) {
      args.push('--limit', String(options.limit));
    }
    
    const output = await runPythonScript('graph_api.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Get node neighbors
ipcMain.handle('graph:neighbors', async (event, options = {}) => {
  try {
    const args = ['neighbors', options.nodeId];
    if (options.nodeType) {
      args.push('--type', options.nodeType);
    }
    if (options.hops) {
      args.push('--hops', String(options.hops));
    }
    if (options.maxNodes) {
      args.push('--max', String(options.maxNodes));
    }
    if (options.includeTypes && options.includeTypes.length > 0) {
      args.push('--include', ...options.includeTypes);
    }
    
    const output = await runPythonScript('graph_api.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Get node details
ipcMain.handle('graph:node-details', async (event, nodeId, nodeType = null) => {
  try {
    const args = ['details', nodeId];
    if (nodeType) {
      args.push('--type', nodeType);
    }
    
    const output = await runPythonScript('graph_api.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Search nodes
ipcMain.handle('graph:search', async (event, query, options = {}) => {
  try {
    const args = ['search', query];
    if (options.types && options.types.length > 0) {
      args.push('--types', ...options.types);
    }
    if (options.limit) {
      args.push('--limit', String(options.limit));
    }
    
    const output = await runPythonScript('graph_api.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Get graph schema
ipcMain.handle('graph:schema', async () => {
  try {
    const output = await runPythonScript('graph_api.py', ['schema']);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Create node
ipcMain.handle('graph:create-node', async (event, nodeType, name, properties = {}) => {
  try {
    const args = ['create-node', nodeType, name];
    if (Object.keys(properties).length > 0) {
      args.push('--props', JSON.stringify(properties));
    }
    
    const output = await runPythonScript('graph_api.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Update node
ipcMain.handle('graph:update-node', async (event, nodeType, name, properties) => {
  try {
    const args = ['update-node', nodeType, name, JSON.stringify(properties)];
    
    const output = await runPythonScript('graph_api.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Delete node
ipcMain.handle('graph:delete-node', async (event, nodeType, name) => {
  try {
    const output = await runPythonScript('graph_api.py', ['delete-node', nodeType, name]);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Create edge
ipcMain.handle('graph:create-edge', async (event, sourceType, sourceName, targetType, targetName, relType, properties = {}) => {
  try {
    const args = ['create-edge', sourceType, sourceName, targetType, targetName, relType];
    if (Object.keys(properties).length > 0) {
      args.push('--props', JSON.stringify(properties));
    }
    
    const output = await runPythonScript('graph_api.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Delete edge
ipcMain.handle('graph:delete-edge', async (event, sourceType, sourceName, targetType, targetName, relType) => {
  try {
    const output = await runPythonScript('graph_api.py', ['delete-edge', sourceType, sourceName, targetType, targetName, relType]);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Apply batch changes
ipcMain.handle('graph:apply-batch', async (event, changes) => {
  try {
    return new Promise((resolve, reject) => {
      const proc = spawnPythonProcess('graph_api.py', ['batch']);
      
      // Send changes as JSON to stdin
      proc.stdin.write(JSON.stringify(changes));
      proc.stdin.end();
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          try {
            resolve(JSON.parse(stdout));
          } catch (e) {
            resolve({ success: false, error: 'Failed to parse response' });
          }
        } else {
          resolve({ success: false, error: stderr || 'Batch operation failed' });
        }
      });
      
      proc.on('error', (err) => {
        resolve({ success: false, error: err.message });
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// AI: Propose relationship changes (with streaming)
ipcMain.handle('graph:ai-propose', async (event, description) => {
  const streamId = `ai-propose-${Date.now()}`;
  
  try {
    return new Promise((resolve, reject) => {
      const proc = spawnPythonProcess('claude_client.py', ['--propose-relationship']);
      
      // Send description as JSON
      const payload = JSON.stringify({ description });
      proc.stdin.write(payload);
      proc.stdin.end();
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => {
        stdout += data.toString();
      });
      
      proc.stderr.on('data', (data) => {
        const text = data.toString();
        stderr += text;
        
        // Stream tool calls to frontend
        if (canSendToRenderer() && text.includes('[TOOL]')) {
          const lines = text.split('\n');
          for (const line of lines) {
            if (line.startsWith('[TOOL]')) {
              sendToRenderer('tool-call', {
                streamId,
                tool: line.replace('[TOOL]', '').trim()
              }, 'ai-propose stderr tool');
            }
          }
        }
      });
      
      proc.on('close', (code) => {
        if (canSendToRenderer()) {
          sendToRenderer('stream-complete', {
            streamId,
            success: code === 0
          }, 'ai-propose close');
        }
        
        if (code === 0) {
          try {
            const result = JSON.parse(stdout);
            resolve({ success: true, ...result, streamId });
          } catch (e) {
            resolve({ success: false, error: 'Failed to parse AI response', streamId });
          }
        } else {
          resolve({ success: false, error: stderr || 'AI proposal failed', streamId });
        }
      });
      
      proc.on('error', (err) => {
        resolve({ success: false, error: err.message, streamId });
      });
    });
  } catch (error) {
    return { success: false, error: error.message, streamId };
  }
});

// AI: Explain selected nodes/edges
ipcMain.handle('graph:ai-explain', async (event, nodeNames) => {
  try {
    const output = await runPythonScript('claude_client.py', ['--explain-nodes', ...nodeNames]);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// ============================================
// DEXPI P&ID Conversion IPC Handlers
// ============================================

// Convert ontology to DEXPI-classified graph
ipcMain.handle('dexpi:convert', async (event, options = {}) => {
  try {
    const args = ['convert'];
    if (options.types && options.types.length > 0) {
      args.push('--types', ...options.types);
    }
    if (options.limit) {
      args.push('--limit', String(options.limit));
    }
    if (options.includeScada) {
      args.push('--include-scada');
    }
    if (options.includeTroubleshooting) {
      args.push('--include-troubleshooting');
    }
    
    const output = await runPythonScript('dexpi_converter.py', args);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Export ontology as DEXPI JSON
ipcMain.handle('dexpi:export', async (event) => {
  try {
    // Show save dialog
    const result = await dialog.showSaveDialog(mainWindow, {
      title: 'Export DEXPI JSON',
      defaultPath: `dexpi_export_${new Date().toISOString().split('T')[0]}.json`,
      filters: [
        { name: 'JSON Files', extensions: ['json'] },
        { name: 'All Files', extensions: ['*'] }
      ]
    });
    
    if (result.canceled || !result.filePath) {
      return { success: false, error: 'Export cancelled' };
    }
    
    const output = await runPythonScript('dexpi_converter.py', ['export', '-o', result.filePath]);
    const parsed = JSON.parse(output);
    return { ...parsed, outputFile: result.filePath };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Get DEXPI legend/schema
ipcMain.handle('dexpi:legend', async () => {
  try {
    const output = await runPythonScript('dexpi_converter.py', ['legend']);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Check pydexpi availability
ipcMain.handle('dexpi:check', async () => {
  try {
    const output = await runPythonScript('dexpi_converter.py', ['check']);
    return JSON.parse(output);
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// ============================================
// Settings IPC Handlers
// ============================================

function getEnvPath() {
  if (pyConfig && pyConfig.packaged) {
    return path.join(pyConfig.cwd, '.env');
  }
  return path.join(pyConfig ? pyConfig.cwd : path.join(__dirname, '..'), '.env');
}

function parseEnvFile(content) {
  const result = {};
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    // Match both active and commented-out lines
    const match = trimmed.match(/^#?\s*([\w]+)\s*=\s*(.*)/);
    if (match) {
      const key = match[1];
      const value = match[2].trim();
      const isCommented = trimmed.startsWith('#');
      result[key] = { value, commented: isCommented };
    }
  }
  return result;
}

// Get current settings from .env
ipcMain.handle('get-settings', async () => {
  try {
    const envPath = getEnvPath();
    if (!fs.existsSync(envPath)) {
      return { success: true, ignitionApiUrl: '', ignitionApiToken: '' };
    }
    const content = fs.readFileSync(envPath, 'utf-8');
    const parsed = parseEnvFile(content);

    return {
      success: true,
      ignitionApiUrl: parsed.IGNITION_API_URL && !parsed.IGNITION_API_URL.commented
        ? parsed.IGNITION_API_URL.value : '',
      ignitionApiToken: parsed.IGNITION_API_TOKEN && !parsed.IGNITION_API_TOKEN.commented
        ? parsed.IGNITION_API_TOKEN.value : '',
    };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Save settings to .env
ipcMain.handle('save-settings', async (event, settings) => {
  try {
    const envPath = getEnvPath();
    let content = '';
    if (fs.existsSync(envPath)) {
      content = fs.readFileSync(envPath, 'utf-8');
    }

    const { ignitionApiUrl, ignitionApiToken } = settings;

    // Helper: update or insert a key in .env content
    function upsertEnvVar(text, key, value) {
      // Match existing line (commented or not)
      const regex = new RegExp(`^#?\\s*${key}\\s*=.*$`, 'm');
      const newLine = value ? `${key}=${value}` : `# ${key}=`;

      if (regex.test(text)) {
        return text.replace(regex, newLine);
      }
      // Append if not found
      const sep = text.endsWith('\n') ? '' : '\n';
      return text + sep + newLine + '\n';
    }

    content = upsertEnvVar(content, 'IGNITION_API_URL', ignitionApiUrl);
    content = upsertEnvVar(content, 'IGNITION_API_TOKEN', ignitionApiToken);

    fs.writeFileSync(envPath, content, 'utf-8');
    return { success: true };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Test Ignition gateway connectivity
ipcMain.handle('test-ignition-connection', async (event, options) => {
  const { url, token } = options;
  if (!url) {
    return { success: false, error: 'No URL provided' };
  }

  const http = url.startsWith('https') ? require('https') : require('http');
  const testUrl = new URL('/data/api/v1/overview', url.replace(/\/+$/, ''));

  return new Promise((resolve) => {
    const headers = { 'Accept': 'application/json' };
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }

    // Force IPv4 when host is localhost — Node.js v17+ defaults to IPv6
    // which fails when the gateway only listens on 127.0.0.1
    const reqOptions = {
      hostname: testUrl.hostname,
      port: testUrl.port,
      path: testUrl.pathname,
      headers,
      timeout: 8000,
    };
    if (testUrl.hostname === 'localhost') {
      reqOptions.hostname = '127.0.0.1';
    }

    const req = http.get(reqOptions, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            const data = JSON.parse(body);
            resolve({
              success: true,
              version: data.version || null,
              state: data.state || null,
              platform: data.platform || null,
            });
          } catch (e) {
            resolve({ success: true, version: null, state: 'Reachable (non-JSON response)' });
          }
        } else {
          resolve({ success: false, error: `HTTP ${res.statusCode}: ${res.statusMessage}` });
        }
      });
    });

    req.on('error', (err) => {
      resolve({ success: false, error: err.message });
    });

    req.on('timeout', () => {
      req.destroy();
      resolve({ success: false, error: 'Connection timed out (8s)' });
    });
  });
});

// ============================================
// Database Connection IPC Handlers
// ============================================

function getDbCredentialsPath() {
  const envDir = pyConfig ? pyConfig.cwd : path.join(__dirname, '..');
  return path.join(envDir, 'db_credentials.json');
}

function readDbCredentials() {
  const credPath = getDbCredentialsPath();
  if (!fs.existsSync(credPath)) return {};
  try {
    return JSON.parse(fs.readFileSync(credPath, 'utf-8'));
  } catch {
    return {};
  }
}

// Get database connections from Neo4j + credential status from db_credentials.json
ipcMain.handle('get-db-connections', async () => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['db-connections', '--json']);

      let stdout = '';

      proc.stdout.on('data', (data) => { stdout += data.toString(); });

      proc.on('close', (code) => {
        if (code !== 0) {
          resolve({ success: true, connections: [] });
          return;
        }
        try {
          const connections = JSON.parse(stdout.trim());
          const creds = readDbCredentials();

          const enriched = connections.map(c => ({
            ...c,
            hasPassword: !!(creds[c.name] && creds[c.name].password),
            savedUsername: creds[c.name] ? creds[c.name].username || '' : '',
          }));

          resolve({ success: true, connections: enriched });
        } catch {
          resolve({ success: true, connections: [] });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Save database credentials to db_credentials.json
ipcMain.handle('save-db-credentials', async (event, credentials) => {
  try {
    const credPath = getDbCredentialsPath();
    const existing = readDbCredentials();

    for (const [name, cred] of Object.entries(credentials)) {
      existing[name] = {
        username: cred.username || '',
        password: cred.password || '',
      };
    }

    fs.writeFileSync(credPath, JSON.stringify(existing, null, 2), 'utf-8');
    return { success: true };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Test a database connection via Python
ipcMain.handle('test-db-connection', async (event, connectionName) => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('db_client_test.py', [connectionName]);

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });

      proc.on('close', (code) => {
        try {
          const result = JSON.parse(stdout.trim());
          resolve(result);
        } catch {
          resolve({
            success: code === 0,
            error: code !== 0 ? (stderr.trim() || 'Connection test failed') : null,
          });
        }
      });
    });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// ============================================
// Long-running Agent Monitoring IPC Handlers
// ============================================

ipcMain.handle('agents:start', async (event, rawConfig = {}) => {
  if (activeAgentRun && activeAgentRun.process && !activeAgentRun.process.killed) {
    return { success: false, error: `Agent run already active: ${activeAgentRun.runId}`, runId: activeAgentRun.runId };
  }

  const runId = `agent-${Date.now()}`;
  const config = normalizeAgentConfig(rawConfig);

  try {
    const proc = spawnPythonProcess('anomaly_monitor.py', [
      'run',
      '--run-id',
      runId,
      '--config-json',
      JSON.stringify(config),
    ]);

    activeAgentRun = {
      runId,
      process: proc,
      status: 'starting',
      startedAt: new Date().toISOString(),
      metrics: {
        cycleMs: 0,
        candidates: 0,
        triaged: 0,
        emitted: 0,
        timestamp: new Date().toISOString(),
      },
      stdoutBuffer: '',
      config,
    };

    proc.stdout.on('data', (data) => {
      handleAgentStdoutChunk(data.toString());
    });

    proc.stderr.on('data', (data) => {
      const text = data.toString().trim();
      if (!text) return;
      console.warn('[Agent stderr]', text.slice(0, 500));
      routeAgentMessage('agent-error', {
        runId,
        code: 'worker_stderr',
        message: text,
        recoverable: true,
        timestamp: new Date().toISOString(),
      });
    });

    proc.on('close', (code) => {
      const hadActive = activeAgentRun && activeAgentRun.runId === runId;
      if (hadActive) {
        routeAgentMessage('agent-complete', {
          runId,
          success: code === 0,
          reason: code === 0 ? 'completed' : 'worker_exit_error',
          stoppedAt: new Date().toISOString(),
        });
        activeAgentRun = null;
      }
    });

    proc.on('error', (err) => {
      routeAgentMessage('agent-error', {
        runId,
        code: 'worker_spawn_error',
        message: err.message,
        recoverable: false,
        timestamp: new Date().toISOString(),
      });
      activeAgentRun = null;
    });

    return { success: true, runId, startedAt: activeAgentRun.startedAt, config };
  } catch (error) {
    activeAgentRun = null;
    return { success: false, error: error.message, runId };
  }
});

ipcMain.handle('agents:status', async (event, runId) => {
  if (activeAgentRun && (!runId || runId === activeAgentRun.runId)) {
    return {
      success: true,
      runId: activeAgentRun.runId,
      status: activeAgentRun.status,
      metrics: activeAgentRun.metrics,
      lastHeartbeatAt: activeAgentRun.metrics.timestamp,
      startedAt: activeAgentRun.startedAt,
      config: activeAgentRun.config,
      active: true,
    };
  }

  if (!runId) {
    return { success: true, active: false, status: 'idle' };
  }

  try {
    const output = await runPythonScript('anomaly_monitor.py', ['status', '--run-id', runId]);
    const parsed = JSON.parse(output || '{}');
    return parsed;
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('agents:stop', async (event, runId = null) => {
  if (!activeAgentRun) {
    return { success: false, error: 'No active agent run' };
  }
  if (runId && runId !== activeAgentRun.runId) {
    return { success: false, error: `Requested run ${runId} does not match active run ${activeAgentRun.runId}` };
  }
  return stopActiveAgent('stopped_by_user');
});

ipcMain.handle('agents:list-events', async (event, filters = {}) => {
  const args = ['list-events'];
  if (filters.limit) args.push('--limit', String(filters.limit));
  if (filters.state) args.push('--state', String(filters.state));
  if (filters.severity) args.push('--severity', String(filters.severity));
  if (filters.runId) args.push('--run-id', String(filters.runId));

  try {
    const output = await runPythonScript('anomaly_monitor.py', args);
    return JSON.parse(output || '{"success":true,"events":[]}');
  } catch (error) {
    return { success: false, error: error.message, events: [] };
  }
});

ipcMain.handle('agents:get-event', async (event, eventId) => {
  try {
    const output = await runPythonScript('anomaly_monitor.py', ['get-event', '--event-id', String(eventId)]);
    return JSON.parse(output || '{}');
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('agents:ack-event', async (event, eventId, note = '') => {
  try {
    const args = ['ack-event', '--event-id', String(eventId)];
    if (note) args.push('--note', String(note));
    const output = await runPythonScript('anomaly_monitor.py', args);
    return JSON.parse(output || '{}');
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('agents:clear-event', async (event, eventId, note = '') => {
  try {
    const args = ['clear-event', '--event-id', String(eventId)];
    if (note) args.push('--note', String(note));
    const output = await runPythonScript('anomaly_monitor.py', args);
    return JSON.parse(output || '{}');
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('agents:deep-analyze', async (event, eventId, eventData) => {
  if (!activeAgentRun || !activeAgentRun.process || activeAgentRun.process.killed) {
    return { success: false, error: 'No active agent run — deep analyze requires a running agent' };
  }
  if (!eventData || !eventData.event_id) {
    return { success: false, error: 'Missing event data' };
  }
  const sent = sendAgentCommand({ cmd: 'deep-analyze', event: eventData });
  if (!sent) {
    return { success: false, error: 'Failed to send command to agent process' };
  }
  return { success: true, pending: true, eventId: eventData.event_id };
});

ipcMain.handle('agents:cleanup', async (event, retentionDays = 14) => {
  try {
    const output = await runPythonScript('anomaly_monitor.py', [
      'cleanup',
      '--retention-days',
      String(retentionDays),
    ]);
    return JSON.parse(output || '{}');
  } catch (error) {
    return { success: false, error: error.message };
  }
});

function sendAgentCommand(cmd) {
  if (activeAgentRun && activeAgentRun.process && activeAgentRun.process.stdin && activeAgentRun.process.stdin.writable) {
    activeAgentRun.process.stdin.write(JSON.stringify(cmd) + '\n');
    return true;
  }
  return false;
}

ipcMain.handle('agents:start-subsystem', async (event, subsystemId) => {
  if (!activeAgentRun) return { success: false, error: 'No active agent run' };
  const sent = sendAgentCommand({ cmd: 'start-agent', subsystemId });
  return { success: sent, subsystemId };
});

ipcMain.handle('agents:stop-subsystem', async (event, subsystemId) => {
  if (!activeAgentRun) return { success: false, error: 'No active agent run' };
  const sent = sendAgentCommand({ cmd: 'stop-agent', subsystemId });
  return { success: sent, subsystemId };
});

// ============================================
// Investigation Cases IPC Handlers
// ============================================

ipcMain.handle('cases:list', async (event, filters = {}, options = {}) => {
  const args = ['list'];
  if (filters.limit) args.push('--limit', String(filters.limit));
  if (filters.status) args.push('--status', String(filters.status));
  try {
    const output = await runPythonScript('case_api.py', args, {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: true, cases: [] });
  } catch (error) {
    return { success: false, error: error.message, cases: [] };
  }
});

ipcMain.handle('cases:get', async (event, caseId, options = {}) => {
  try {
    const output = await runPythonScript('case_api.py', ['get', '--case-id', String(caseId)], {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid case response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:create-from-event', async (event, eventPayload = {}, options = {}) => {
  try {
    const output = await runPythonScriptWithStdin('case_api.py', ['create-from-event'], eventPayload, {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid case response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:update', async (event, caseId, patch = {}, options = {}) => {
  try {
    const output = await runPythonScriptWithStdin('case_api.py', ['update', '--case-id', String(caseId)], patch, {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid case response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:delete', async (event, caseId, options = {}) => {
  try {
    const output = await runPythonScript('case_api.py', ['delete', '--case-id', String(caseId)], {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid delete response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:generate-draft', async (event, caseId, options = {}) => {
  try {
    const output = await runPythonScript('case_api.py', ['generate-draft', '--case-id', String(caseId)], {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid draft response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:approve-draft', async (event, caseId, options = {}) => {
  try {
    const output = await runPythonScript('case_api.py', ['approve-draft', '--case-id', String(caseId)], {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid draft response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:reject-draft', async (event, caseId, options = {}) => {
  try {
    const output = await runPythonScript('case_api.py', ['reject-draft', '--case-id', String(caseId)], {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid draft response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:generate-report', async (event, caseId, options = {}) => {
  try {
    const output = await runPythonScript('case_api.py', ['generate-report', '--case-id', String(caseId)], {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'default',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid report response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:assistant-summarize', async (event, history, turns, context, options = {}) => {
  try {
    const output = await runPythonScriptWithStdin('troubleshoot.py', ['--history', '-v'], {
      mode: 'summarize_case_transcript',
      history: history || [],
      turns: turns || [],
      context: context || '',
    }, {
      streaming: Boolean(options.streamId),
      streamId: options.streamId || null,
      target: options.target || 'cases',
    });
    return parseJsonFromMixedOutput(output, { success: false, error: 'Invalid case summary response' });
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('cases:save-report', async (event, suggestedFilename, markdown) => {
  try {
    const result = await dialog.showSaveDialog(mainWindow, {
      title: 'Save Investigation Report',
      defaultPath: suggestedFilename || 'investigation_report.md',
      filters: [
        { name: 'Markdown', extensions: ['md'] },
        { name: 'Text', extensions: ['txt'] },
        { name: 'All Files', extensions: ['*'] },
      ],
    });
    if (result.canceled || !result.filePath) return { success: false, cancelled: true };
    fs.writeFileSync(result.filePath, String(markdown || ''), 'utf-8');
    return { success: true, filePath: result.filePath };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// ============================================
// Artifact Ingestion IPC (P&IDs / SOPs / Diagrams via GPT-5.4)
// ============================================

ipcMain.handle('ingest-artifact', async (event, filePath, sourceKind = 'pid') => {
  try {
    sendToRenderer('stream-output', { text: `Ingesting ${path.basename(filePath)} as ${sourceKind}...\n` });
    const output = await runPythonScript('artifact_ingest.py', [
      filePath,
      '--source-kind', sourceKind,
      '--verbose',
      '--json',
    ], { streaming: true, streamId: 'artifact-ingest' });
    const result = JSON.parse(output || '{}');
    return { success: true, ...result };
  } catch (error) {
    sendToRenderer('stream-output', { text: `Ingestion error: ${error.message}\n` });
    return { success: false, error: error.message };
  }
});

ipcMain.handle('ingest-artifact-batch', async (event, files) => {
  try {
    const filePaths = files.map(f => f.path);
    const sourceKind = files[0]?.sourceKind || 'pid';
    sendToRenderer('stream-output', { text: `Ingesting ${files.length} artifact(s)...\n` });
    const output = await runPythonScript('artifact_ingest.py', [
      ...filePaths,
      '--source-kind', sourceKind,
      '--verbose',
      '--json',
    ], { streaming: true, streamId: 'artifact-ingest' });
    const result = JSON.parse(output || '{}');
    return { success: true, ...result };
  } catch (error) {
    return { success: false, error: error.message };
  }
});