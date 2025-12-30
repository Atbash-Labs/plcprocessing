const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const { spawn } = require('child_process');

let mainWindow;

// Path to scripts directory (relative to project root)
const scriptsDir = path.join(__dirname, '..', 'scripts');

function createWindow() {
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
    const scriptPath = path.join(scriptsDir, scriptName);
    // Use -u for unbuffered output to enable real-time streaming
    const pythonProcess = spawn('python', ['-u', scriptPath, ...args], {
      cwd: path.join(__dirname, '..'),
      env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' }
    });

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
      { name: 'All Supported', extensions: ['json', 'sc', 'L5X'] },
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'PLC Files', extensions: ['sc', 'L5X'] }
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

// Ingest PLC files (with streaming)
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

// Ingest Ignition files (with streaming)
ipcMain.handle('ingest-ignition', async (event, filePath) => {
  const streamId = `ingest-ignition-${Date.now()}`;
  try {
    const output = await runPythonScript('ignition_ontology.py', [filePath, '-v'], {
      streaming: true,
      streamId
    });
    return { success: true, output, streamId };
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
    const scriptPath = path.join(scriptsDir, 'neo4j_ontology.py');
    
    return new Promise((resolve, reject) => {
      const proc = spawn('python', ['-u', scriptPath, 'clear'], {
        cwd: path.join(__dirname, '..'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' }
      });
      
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
    const scriptPath = path.join(scriptsDir, 'troubleshoot.py');
    
    // Prepare JSON payload with question and history
    const payload = JSON.stringify({
      question: question,
      history: history || []
    });
    
    return new Promise((resolve, reject) => {
      // Use -u for unbuffered output to enable real-time streaming
      const proc = spawn('python', ['-u', scriptPath, '--history', '-v'], {
        cwd: path.join(__dirname, '..'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' }
      });
      
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
        
        // Stream tool calls and debug info from stderr to frontend
        if (mainWindow) {
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
    const outputPath = path.join(__dirname, '..', 'ontology_graph.html');
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
    const scriptPath = path.join(scriptsDir, 'neo4j_ontology.py');
    
    return new Promise((resolve) => {
      const proc = spawn('python', ['-u', scriptPath, 'load', '-f', filePath, '--yes'], {
        cwd: path.join(__dirname, '..'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' }
      });
      
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

