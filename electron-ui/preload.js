const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods to renderer
contextBridge.exposeInMainWorld('api', {
  // File dialogs
  selectFile: (options) => ipcRenderer.invoke('select-file', options),
  selectDirectory: () => ipcRenderer.invoke('select-directory'),
  
  // Ingestion (with streaming support)
  ingestPLC: (filePath) => ipcRenderer.invoke('ingest-plc', filePath),
  ingestIgnition: (filePath) => ipcRenderer.invoke('ingest-ignition', filePath),
  
  // Analysis
  runUnified: () => ipcRenderer.invoke('run-unified'),
  runEnrichment: () => ipcRenderer.invoke('run-enrichment'),
  
  // Database
  clearDatabase: () => ipcRenderer.invoke('clear-database'),
  initDatabase: () => ipcRenderer.invoke('init-database'),
  getStats: () => ipcRenderer.invoke('get-stats'),
  
  // Troubleshooting with conversation history (with streaming support)
  troubleshoot: (question, history) => ipcRenderer.invoke('troubleshoot', question, history),
  
  // Visualization
  generateViz: () => ipcRenderer.invoke('generate-viz'),
  
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

