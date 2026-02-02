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
  
  // Graph API
  graphLoad: (options) => ipcRenderer.invoke('graph:load', options),
  graphNeighbors: (options) => ipcRenderer.invoke('graph:neighbors', options),
  graphNodeDetails: (nodeId, nodeType) => ipcRenderer.invoke('graph:node-details', nodeId, nodeType),
  graphSearch: (query, options) => ipcRenderer.invoke('graph:search', query, options),
  graphSchema: () => ipcRenderer.invoke('graph:schema'),
  graphCreateNode: (nodeType, name, properties) => ipcRenderer.invoke('graph:create-node', nodeType, name, properties),
  graphUpdateNode: (nodeType, name, properties) => ipcRenderer.invoke('graph:update-node', nodeType, name, properties),
  graphDeleteNode: (nodeType, name) => ipcRenderer.invoke('graph:delete-node', nodeType, name),
  graphCreateEdge: (sourceType, sourceName, targetType, targetName, relType, properties) => 
    ipcRenderer.invoke('graph:create-edge', sourceType, sourceName, targetType, targetName, relType, properties),
  graphDeleteEdge: (sourceType, sourceName, targetType, targetName, relType) => 
    ipcRenderer.invoke('graph:delete-edge', sourceType, sourceName, targetType, targetName, relType),
  graphApplyBatch: (changes) => ipcRenderer.invoke('graph:apply-batch', changes),
  graphAiPropose: (description) => ipcRenderer.invoke('graph:ai-propose', description),
  graphAiExplain: (nodeNames) => ipcRenderer.invoke('graph:ai-explain', nodeNames),
  
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

