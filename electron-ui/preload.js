const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods to renderer
contextBridge.exposeInMainWorld('api', {
  // File dialogs
  selectFile: (options) => ipcRenderer.invoke('select-file', options),
  selectDirectory: () => ipcRenderer.invoke('select-directory'),
  
  // Ingestion
  ingestPLC: (filePath) => ipcRenderer.invoke('ingest-plc', filePath),
  ingestSiemens: (filePath) => ipcRenderer.invoke('ingest-siemens', filePath),
  ingestTiaXml: (filePath) => ipcRenderer.invoke('ingest-tia-xml', filePath),
  ingestTiaProject: (folderPath) => ipcRenderer.invoke('ingest-tia-project', folderPath),
  ingestIgnition: (filePath) => ipcRenderer.invoke('ingest-ignition', filePath),
  ingestWorkbench: (folderPath) => ipcRenderer.invoke('ingest-workbench', folderPath),
  
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
  
  // TIA Portal Browse API
  getTiaProjects: () => ipcRenderer.invoke('get-tia-projects'),
  getTiaProjectResources: (projectName) => ipcRenderer.invoke('get-tia-project-resources', projectName),
  enrichTiaBatch: (options) => ipcRenderer.invoke('enrich-tia-batch', options),
  
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
  
  // DEXPI P&ID Conversion API
  dexpiConvert: (options) => ipcRenderer.invoke('dexpi:convert', options),
  dexpiExport: () => ipcRenderer.invoke('dexpi:export'),
  dexpiLegend: () => ipcRenderer.invoke('dexpi:legend'),
  dexpiCheck: () => ipcRenderer.invoke('dexpi:check'),
  
  // Settings
  getSettings: () => ipcRenderer.invoke('get-settings'),
  saveSettings: (settings) => ipcRenderer.invoke('save-settings', settings),
  testIgnitionConnection: (options) => ipcRenderer.invoke('test-ignition-connection', options),

  // Long-running agents monitoring
  agentsStart: (config) => ipcRenderer.invoke('agents:start', config),
  agentsStatus: (runId) => ipcRenderer.invoke('agents:status', runId),
  agentsStop: (runId) => ipcRenderer.invoke('agents:stop', runId),
  agentsListEvents: (filters) => ipcRenderer.invoke('agents:list-events', filters),
  agentsGetEvent: (eventId) => ipcRenderer.invoke('agents:get-event', eventId),
  agentsAckEvent: (eventId, note) => ipcRenderer.invoke('agents:ack-event', eventId, note),
  agentsClearEvent: (eventId, note) => ipcRenderer.invoke('agents:clear-event', eventId, note),
  agentsDeepAnalyze: (eventId) => ipcRenderer.invoke('agents:deep-analyze', eventId),
  agentsCleanup: (retentionDays) => ipcRenderer.invoke('agents:cleanup', retentionDays),
  
  // Database connections
  getDbConnections: () => ipcRenderer.invoke('get-db-connections'),
  saveDbCredentials: (credentials) => ipcRenderer.invoke('save-db-credentials', credentials),
  testDbConnection: (connectionName) => ipcRenderer.invoke('test-db-connection', connectionName),
  
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
  },
  onAgentStatus: (callback) => {
    const handler = (event, data) => callback(data);
    ipcRenderer.on('agent-status', handler);
    return () => ipcRenderer.removeListener('agent-status', handler);
  },
  onAgentEvent: (callback) => {
    const handler = (event, data) => callback(data);
    ipcRenderer.on('agent-event', handler);
    return () => ipcRenderer.removeListener('agent-event', handler);
  },
  onAgentError: (callback) => {
    const handler = (event, data) => callback(data);
    ipcRenderer.on('agent-error', handler);
    return () => ipcRenderer.removeListener('agent-error', handler);
  },
  onAgentComplete: (callback) => {
    const handler = (event, data) => callback(data);
    ipcRenderer.on('agent-complete', handler);
    return () => ipcRenderer.removeListener('agent-complete', handler);
  }
});

