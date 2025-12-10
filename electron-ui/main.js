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

// Helper to run Python scripts
function runPythonScript(scriptName, args = [], onData = null) {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(scriptsDir, scriptName);
    const pythonProcess = spawn('python', [scriptPath, ...args], {
      cwd: path.join(__dirname, '..'),
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    let stdout = '';
    let stderr = '';

    pythonProcess.stdout.on('data', (data) => {
      const text = data.toString();
      stdout += text;
      if (onData) onData(text);
    });

    pythonProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    pythonProcess.on('close', (code) => {
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

// Ingest PLC files
ipcMain.handle('ingest-plc', async (event, filePath) => {
  try {
    const output = await runPythonScript('ontology_analyzer.py', [filePath, '-v']);
    return { success: true, output };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Ingest Ignition files
ipcMain.handle('ingest-ignition', async (event, filePath) => {
  try {
    const output = await runPythonScript('ignition_ontology.py', [filePath, '-v']);
    return { success: true, output };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Run unified analysis
ipcMain.handle('run-unified', async () => {
  try {
    const output = await runPythonScript('unified_ontology.py', ['--analyze', '-v']);
    return { success: true, output };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Run troubleshooting enrichment
ipcMain.handle('run-enrichment', async () => {
  try {
    const output = await runPythonScript('troubleshooting_ontology.py', ['--enrich-all', '-v']);
    return { success: true, output };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

// Clear database
ipcMain.handle('clear-database', async () => {
  try {
    // Use neo4j_ontology.py with 'yes' piped to stdin
    const scriptPath = path.join(scriptsDir, 'neo4j_ontology.py');
    
    return new Promise((resolve, reject) => {
      const proc = spawn('python', [scriptPath, 'clear'], {
        cwd: path.join(__dirname, '..'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
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

// Troubleshooting query with conversation history
ipcMain.handle('troubleshoot', async (event, question, history) => {
  try {
    const scriptPath = path.join(scriptsDir, 'troubleshoot.py');
    
    // Prepare JSON payload with question and history
    const payload = JSON.stringify({
      question: question,
      history: history || []
    });
    
    return new Promise((resolve, reject) => {
      const proc = spawn('python', [scriptPath, '--history'], {
        cwd: path.join(__dirname, '..'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      
      // Send JSON to stdin
      proc.stdin.write(payload);
      proc.stdin.end();
      
      let stdout = '';
      let stderr = '';
      
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });
      
      proc.on('close', (code) => {
        if (code === 0) {
          try {
            const result = JSON.parse(stdout);
            resolve({ 
              success: true, 
              response: result.response,
              history: result.history 
            });
          } catch (e) {
            // Fallback if JSON parsing fails
            resolve({ success: true, response: stdout, history: [] });
          }
        } else {
          resolve({ success: false, error: stderr || 'Query failed' });
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

