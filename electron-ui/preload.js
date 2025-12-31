const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods to renderer
contextBridge.exposeInMainWorld('api', {
  // File dialogs
  selectFile: (options) => ipcRenderer.invoke('select-file', options),
  selectDirectory: () => ipcRenderer.invoke('select-directory'),
  
  // Ingestion
  ingestPLC: (filePath) => ipcRenderer.invoke('ingest-plc', filePath),
  ingestIgnition: (filePath) => ipcRenderer.invoke('ingest-ignition', filePath),
  
  // Analysis
  runUnified: () => ipcRenderer.invoke('run-unified'),
  runEnrichment: () => ipcRenderer.invoke('run-enrichment'),
  
  // Database
  clearDatabase: () => ipcRenderer.invoke('clear-database'),
  initDatabase: () => ipcRenderer.invoke('init-database'),
  getStats: () => ipcRenderer.invoke('get-stats'),
  saveDatabase: () => ipcRenderer.invoke('save-database'),
  loadDatabase: () => ipcRenderer.invoke('load-database'),
  
  // Troubleshooting with conversation history
  troubleshoot: (question, history) => ipcRenderer.invoke('troubleshoot', question, history),
  
  // Visualization
  generateViz: () => ipcRenderer.invoke('generate-viz'),
  
  // Browse Tab - Projects and Resources
  getProjects: () => ipcRenderer.invoke('get-projects'),
  getGatewayResources: () => ipcRenderer.invoke('get-gateway-resources'),
  getProjectResources: (projectName) => ipcRenderer.invoke('get-project-resources', projectName),
  getEnrichmentStatus: (options) => ipcRenderer.invoke('get-enrichment-status', options),
  enrichBatch: (options) => ipcRenderer.invoke('enrich-batch', options),
  
  // Event listeners for streaming (returns cleanup function)
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

