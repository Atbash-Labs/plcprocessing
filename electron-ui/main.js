const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');

let mainWindow;

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

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});

// Helper to run Python scripts with optional streaming
function runPythonScript(scriptName, args = [], options = {}) {
  const { streaming = false, streamId = null } = options;
  
  return new Promise((resolve, reject) => {
    const pythonProcess = spawnPythonProcess(scriptName, args);

    let stdout = '';
    let stderr = '';

    pythonProcess.stdout.on('data', (data) => {
      const text = data.toString();
      stdout += text;
      
      // Send streaming output to renderer if enabled
      if (streaming && mainWindow) {
        // Parse and emit tool calls separately
        const lines = text.split('\n');
        for (const line of lines) {
          if (line.startsWith('[TOOL]')) {
            mainWindow.webContents.send('tool-call', {
              streamId,
              tool: line.replace('[TOOL]', '').trim()
            });
          } else if (line.startsWith('[DEBUG]')) {
            mainWindow.webContents.send('stream-output', {
              streamId,
              text: line,
              type: 'debug'
            });
          } else if (line.trim()) {
            mainWindow.webContents.send('stream-output', {
              streamId,
              text: line,
              type: 'output'
            });
          }
        }
      }
    });

    pythonProcess.stderr.on('data', (data) => {
      const text = data.toString();
      stderr += text;
      
      // Stream stderr too (useful for verbose output)
      if (streaming && mainWindow) {
        mainWindow.webContents.send('stream-output', {
          streamId,
          text,
          type: 'stderr'
        });
      }
    });

    pythonProcess.on('close', (code) => {
      if (streaming && mainWindow) {
        mainWindow.webContents.send('stream-complete', {
          streamId,
          success: code === 0
        });
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

// IPC Handlers

// Select file dialog
ipcMain.handle('select-file', async (event, options) => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openFile'],
    filters: options.filters || [
      { name: 'All Supported', extensions: ['json', 'sc', 'L5X', 'st', 'xml'] },
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'Rockwell PLC', extensions: ['sc', 'L5X'] },
      { name: 'Siemens PLC', extensions: ['st'] },
      { name: 'TIA Portal XML', extensions: ['xml'] }
    ]
  });
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
        if (mainWindow) {
          // Check for special prefixes first (they appear on their own lines)
          if (text.includes('[TOOL]') || text.includes('[DEBUG]') || text.includes('[INFO]')) {
            const lines = text.split('\n');
            for (const line of lines) {
              if (line.startsWith('[TOOL]')) {
                mainWindow.webContents.send('tool-call', {
                  streamId,
                  tool: line.replace('[TOOL]', '').trim()
                });
              } else if (line.startsWith('[DEBUG]') || line.startsWith('[INFO]')) {
                mainWindow.webContents.send('stream-output', {
                  streamId,
                  text: line,
                  type: 'debug'
                });
              }
            }
          } else if (text.includes('[STREAM]')) {
            // Start of Claude streaming - send what comes after [STREAM]
            const streamStart = text.indexOf('[STREAM]');
            const afterStream = text.substring(streamStart + 8); // 8 = length of '[STREAM]'
            if (afterStream) {
              mainWindow.webContents.send('stream-output', {
                streamId,
                text: afterStream,
                type: 'claude-stream'
              });
            }
          } else if (text && !text.startsWith('[')) {
            // Continuation of Claude streaming (no prefix)
            mainWindow.webContents.send('stream-output', {
              streamId,
              text: text,
              type: 'claude-stream'
            });
          }
        }
      });
      
      proc.on('close', (code) => {
        if (mainWindow) {
          mainWindow.webContents.send('stream-complete', {
            streamId,
            success: code === 0
          });
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
        if (mainWindow && text.includes('[TOOL]')) {
          const lines = text.split('\n');
          for (const line of lines) {
            if (line.startsWith('[TOOL]')) {
              mainWindow.webContents.send('tool-call', {
                streamId,
                tool: line.replace('[TOOL]', '').trim()
              });
            }
          }
        }
      });
      
      proc.on('close', (code) => {
        if (mainWindow) {
          mainWindow.webContents.send('stream-complete', {
            streamId,
            success: code === 0
          });
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
  } catch { return {}; }
}

// Get database connections from Neo4j + credential status from db_credentials.json
ipcMain.handle('get-db-connections', async () => {
  try {
    return new Promise((resolve) => {
      const proc = spawnPythonProcess('neo4j_ontology.py', ['db-connections', '--json']);

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });

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
        } catch (e) {
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
    let existing = readDbCredentials();

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