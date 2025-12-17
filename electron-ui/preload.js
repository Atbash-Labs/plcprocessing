const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods to renderer
contextBridge.exposeInMainWorld('api', {
  // File dialogs
  selectFile: (options) => ipcRenderer.invoke('select-file', options),
  selectDirectory: () => ipcRenderer.invoke('select-directory'),
  
  // Ingestion (with streaming support)
  // skipAI: if true, only create entities without AI analysis (for incremental mode)
  ingestPLC: (filePath) => ipcRenderer.invoke('ingest-plc', filePath),
  ingestIgnition: (filePath, skipAI = false) => ipcRenderer.invoke('ingest-ignition', filePath, skipAI),
  
  // Analysis
  runUnified: () => ipcRenderer.invoke('run-unified'),
  runEnrichment: (options) => ipcRenderer.invoke('run-enrichment', options),
  runEnrichmentViews: (options) => ipcRenderer.invoke('run-enrichment-views', options),
  getEnrichmentStatus: () => ipcRenderer.invoke('get-enrichment-status'),
  
  // Incremental Semantic Analysis
  getSemanticStatus: () => ipcRenderer.invoke('get-semantic-status'),
  runIncrementalAnalysis: (options) => ipcRenderer.invoke('run-incremental-analysis', options),
  resetSemanticStatus: (itemType) => ipcRenderer.invoke('reset-semantic-status', itemType),
  recoverStuck: () => ipcRenderer.invoke('recover-stuck'),
  
  // Database
  clearDatabase: () => ipcRenderer.invoke('clear-database'),
  clearIgnition: () => ipcRenderer.invoke('clear-ignition'),
  clearPLC: () => ipcRenderer.invoke('clear-plc'),
  clearUnification: () => ipcRenderer.invoke('clear-unification'),
  initDatabase: () => ipcRenderer.invoke('init-database'),
  getStats: () => ipcRenderer.invoke('get-stats'),
  
  // Troubleshooting with conversation history (with streaming support)
  troubleshoot: (question, history) => ipcRenderer.invoke('troubleshoot', question, history),
  
  // Visualization
  generateViz: () => ipcRenderer.invoke('generate-viz'),
  
  // Diff Processing
  selectDiffFile: () => ipcRenderer.invoke('select-diff-file'),
  previewDiff: (diffPath, backupPath) => ipcRenderer.invoke('preview-diff', diffPath, backupPath),
  applyDiff: (diffPath, backupPath) => ipcRenderer.invoke('apply-diff', diffPath, backupPath),
  
  // Streaming output listeners
  onStreamOutput: (callback) => {
    const handler = (event, data) => callback(data);
    ipcRenderer.on('stream-output', handler);
    return () => ipcRenderer.removeListener('stream-output', handler);
  },
  
  onToolCall: (callback) => {
    const handler = (event, data) => callback(data);
    ipcRenderer.on('tool-call', handler);
    return () => ipcRenderer.removeListener('tool-call', handler);
  },
  
  onStreamComplete: (callback) => {
    const handler = (event, data) => callback(data);
    ipcRenderer.on('stream-complete', handler);
    return () => ipcRenderer.removeListener('stream-complete', handler);
  }
});

