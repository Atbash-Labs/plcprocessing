// PLC Ontology Assistant - Renderer Script

// ============================================
// Tab Navigation
// ============================================

const navButtons = document.querySelectorAll('.nav-btn');
const tabContents = document.querySelectorAll('.tab-content');

navButtons.forEach(btn => {
  btn.addEventListener('click', () => {
    const tabId = btn.dataset.tab;
    
    // Update nav buttons
    navButtons.forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    
    // Update tab content
    tabContents.forEach(tab => {
      tab.classList.remove('active');
      if (tab.id === `tab-${tabId}`) {
        tab.classList.add('active');
      }
    });
  });
});

function activateTab(tabId) {
  navButtons.forEach(b => b.classList.remove('active'));
  document.querySelector(`[data-tab="${tabId}"]`)?.classList.add('active');
  tabContents.forEach(tab => {
    tab.classList.remove('active');
    if (tab.id === `tab-${tabId}`) {
      tab.classList.add('active');
    }
  });
}

// ============================================
// Loading Overlay
// ============================================

const loadingOverlay = document.getElementById('loading-overlay');
const loadingText = document.getElementById('loading-text');

function showLoading(text = 'Processing...') {
  loadingText.textContent = text;
  loadingOverlay.classList.add('active');
}

function hideLoading() {
  loadingOverlay.classList.remove('active');
}

// ============================================
// Output Panel
// ============================================

const ingestOutput = document.querySelector('#ingest-output .output-content');

function appendOutput(text, clear = false) {
  if (clear) {
    ingestOutput.textContent = '';
  }
  // Clean up Neo4j deprecation warnings
  const cleaned = text
    .split('\n')
    .filter(line => !line.includes('GqlStatusObject') && !line.includes('Received notification'))
    .join('\n');
  ingestOutput.textContent += cleaned;
  ingestOutput.scrollTop = ingestOutput.scrollHeight;
}

document.getElementById('btn-clear-ingest-output').addEventListener('click', () => {
  ingestOutput.textContent = '';
});

// Clear enrichment output button (if exists - in browse tab)
const clearEnrichmentBtn = document.getElementById('btn-clear-enrichment-output');
if (clearEnrichmentBtn) {
  clearEnrichmentBtn.addEventListener('click', () => {
    const log = document.getElementById('enrichment-log');
    if (log) log.textContent = '';
  });
}

// Load Ignition file for enrichment context (without full ingestion)
const loadIgnitionFileBtn = document.getElementById('btn-load-ignition-file');
if (loadIgnitionFileBtn) {
  loadIgnitionFileBtn.addEventListener('click', async () => {
    const filePath = await window.api.selectFile({
      filters: [
        { name: 'Ignition Backup', extensions: ['json'] }
      ]
    });
    
    if (filePath) {
      browseState.lastIgnitionFile = filePath;
      
      // Update display
      const fileNameSpan = document.getElementById('enrichment-file-name');
      if (fileNameSpan) {
        // Extract just the filename from the path
        const fileName = filePath.split(/[/\\]/).pop();
        fileNameSpan.textContent = fileName;
        fileNameSpan.classList.add('loaded');
        fileNameSpan.title = filePath; // Full path on hover
      }
      
      appendEnrichmentLog(`[LOADED] ${filePath}\n`);
    }
  });
}

// ============================================
// Ingest Tab Handlers
// ============================================

// PLC Ingest (Rockwell)
document.getElementById('btn-ingest-plc').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'Rockwell PLC Files', extensions: ['sc', 'L5X'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  // Don't use loading overlay - we want to see streaming output
  appendOutput(`\n[INGEST ROCKWELL] ${filePath}\n`, false);
  
  try {
    const result = await window.api.ingestPLC(filePath);
    if (result.success) {
      // Don't re-append result.output since it's already streamed
      appendOutput('\n[OK] Rockwell PLC ingestion complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
  await loadProjects();
  await loadGatewayResources();
});

// PLC Ingest (Siemens)
document.getElementById('btn-ingest-siemens').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'Siemens ST Files', extensions: ['st'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  // Don't use loading overlay - we want to see streaming output
  appendOutput(`\n[INGEST SIEMENS] ${filePath}\n`, false);
  
  try {
    const result = await window.api.ingestSiemens(filePath);
    if (result.success) {
      appendOutput('\n[OK] Siemens PLC ingestion complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
  await loadProjects();
  await loadGatewayResources();
});

// Siemens TIA Portal full project ingest
document.getElementById('btn-ingest-tia-project').addEventListener('click', async () => {
  const folderPath = await window.api.selectDirectory();
  if (!folderPath) return;
  
  appendOutput(`\n[INGEST TIA PROJECT] ${folderPath}\n`, false);
  appendOutput(`[INFO] Scanning PLCs, HMIs, tag tables, alarms, scripts, screens, types...\n`);
  
  try {
    const result = await window.api.ingestTiaProject(folderPath);
    if (result.success) {
      appendOutput('\n[OK] Siemens TIA Portal project ingestion complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
  await loadProjects();
  await loadGatewayResources();
});

// PLC Ingest (TIA Portal XML) - single file
document.getElementById('btn-ingest-tia-xml-file').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'TIA Portal XML', extensions: ['xml'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  appendOutput(`\n[INGEST TIA XML] ${filePath}\n`, false);
  
  try {
    const result = await window.api.ingestTiaXml(filePath);
    if (result.success) {
      appendOutput('\n[OK] TIA Portal XML ingestion complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
  await loadProjects();
  await loadGatewayResources();
});

// PLC Ingest (TIA Portal XML) - recursive directory
document.getElementById('btn-ingest-tia-xml-dir').addEventListener('click', async () => {
  const dirPath = await window.api.selectDirectory();
  if (!dirPath) return;
  
  appendOutput(`\n[INGEST TIA XML DIR] ${dirPath}\n`, false);
  appendOutput(`[INFO] Scanning recursively for .xml files...\n`);
  
  try {
    const result = await window.api.ingestTiaXml(dirPath);
    if (result.success) {
      appendOutput('\n[OK] TIA Portal XML directory ingestion complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
  await loadProjects();
  await loadGatewayResources();
});

// Ignition Ingest - handler defined later in Browse section

// Unified Analysis
document.getElementById('btn-run-unified').addEventListener('click', async () => {
  appendOutput('\n[UNIFIED] Running unified analysis (linking PLC <-> SCADA)...\n', false);
  
  try {
    const result = await window.api.runUnified();
    if (result.success) {
      // Don't re-append result.output since it's already streamed
      appendOutput('\n[OK] Unified analysis complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
  await loadProjects();
  await loadGatewayResources();
});

// Troubleshooting Enrichment
document.getElementById('btn-run-enrichment').addEventListener('click', async () => {
  appendOutput('\n[ENRICH] Running troubleshooting enrichment...\n', false);
  
  try {
    const result = await window.api.runEnrichment();
    if (result.success) {
      // Don't re-append result.output since it's already streamed
      appendOutput('\n[OK] Troubleshooting enrichment complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
});

// ============================================
// Chat/Troubleshooting
// ============================================

const chatMessages = document.getElementById('chat-messages');
const chatInput = document.getElementById('chat-input');
const btnSend = document.getElementById('btn-send');

// Maintain conversation history for multi-turn dialogue
let conversationHistory = [];

function clearConversation() {
  conversationHistory = [];
  // Keep only the initial greeting
  const greeting = chatMessages.querySelector('.message.assistant');
  chatMessages.innerHTML = '';
  if (greeting) {
    chatMessages.appendChild(greeting);
  }
}

function addMessage(content, isUser = false) {
  const messageDiv = document.createElement('div');
  messageDiv.className = `message ${isUser ? 'user' : 'assistant'}`;
  
  const contentDiv = document.createElement('div');
  contentDiv.className = 'message-content';
  
  if (isUser) {
    contentDiv.textContent = content;
  } else {
    // Parse markdown-like formatting
    contentDiv.innerHTML = formatResponse(content);
  }
  
  messageDiv.appendChild(contentDiv);
  chatMessages.appendChild(messageDiv);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function formatResponse(text) {
  // Clean up Neo4j warnings first
  text = text
    .split('\n')
    .filter(line => !line.includes('GqlStatusObject') && !line.includes('Received notification'))
    .join('\n');
  
  // Basic markdown formatting
  let formatted = text
    // Headers
    .replace(/^### (.*$)/gm, '<h4>$1</h4>')
    .replace(/^## (.*$)/gm, '<h3>$1</h3>')
    .replace(/^# (.*$)/gm, '<h2>$1</h2>')
    // Bold
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    // Code blocks
    .replace(/```([\s\S]*?)```/g, '<pre>$1</pre>')
    // Inline code - make these graph references
    .replace(/`([^`]+)`/g, (match, name) => {
      return createGraphRef(name);
    })
    // Horizontal rules
    .replace(/^---$/gm, '<hr>')
    // Lists
    .replace(/^- (.*$)/gm, '• $1')
    // Line breaks
    .replace(/\n/g, '<br>');
  
  // Also detect node-like patterns (CamelCase_Names, prefixed names)
  // Match patterns like: View:Dashboard, AOI:MotorStart, PLX-SLS-001, HMIAlarm:MyAlarm
  formatted = formatted.replace(/\b(AOI|View|UDT|Tag|Equipment|SIF|Script|Query|Component|TiaProject|PLCDevice|HMIDevice|HMIConnection|HMIAlarm|HMIAlarmClass|HMIScript|HMIScreen|HMITagTable|HMITextList|PLCTagTable|PLCTag)[:]\s*([A-Za-z0-9_\-\/]+)/gi, (match, type, name) => {
    return `${type}: ${createGraphRef(name, type)}`;
  });
  
  return formatted;
}

// Create a graph reference span for interactive hover
function createGraphRef(name, type = null) {
  // Skip very short names or common words
  const skipWords = ['the', 'and', 'for', 'with', 'this', 'that', 'from', 'true', 'false', 'null', 'name', 'type', 'path'];
  if (name.length < 3 || skipWords.includes(name.toLowerCase())) {
    return `<code>${name}</code>`;
  }
  
  const dataType = type ? `data-type="${type}"` : '';
  return `<span class="graph-ref" data-name="${escapeHtml(name)}" ${dataType}>${name}</span>`;
}

async function sendMessage() {
  const question = chatInput.value.trim();
  if (!question) return;
  
  // Add user message to UI
  addMessage(question, true);
  chatInput.value = '';
  
  // Show thinking indicator with tool calls container and streaming text area
  const thinkingDiv = document.createElement('div');
  thinkingDiv.className = 'message assistant';
  thinkingDiv.innerHTML = `
    <div class="message-content">
      <p class="thinking-text">Analyzing your question...</p>
      <div class="tool-calls-container"></div>
      <div class="streaming-text" style="display: none;"></div>
    </div>
  `;
  chatMessages.appendChild(thinkingDiv);
  chatMessages.scrollTop = chatMessages.scrollHeight;
  
  const toolCallsContainer = thinkingDiv.querySelector('.tool-calls-container');
  const streamingTextDiv = thinkingDiv.querySelector('.streaming-text');
  const thinkingText = thinkingDiv.querySelector('.thinking-text');
  let streamingStarted = false;
  
  // Set up tool call listener for this request (returns cleanup function)
  const cleanupToolCall = window.api.onToolCall((data) => {
    if (data?.target === 'cases-assistant') return;
    const chip = document.createElement('span');
    chip.className = 'tool-call-chip';
    chip.innerHTML = `<span class="tool-icon">&gt;</span> ${data.tool}`;
    toolCallsContainer.appendChild(chip);
    chatMessages.scrollTop = chatMessages.scrollHeight;
  });
  
  // Set up streaming text listener with throttling to prevent UI freeze
  let streamBuffer = '';
  let flushScheduled = false;
  
  const flushStreamBuffer = () => {
    if (streamBuffer) {
      streamingTextDiv.textContent += streamBuffer;
      streamBuffer = '';
      chatMessages.scrollTop = chatMessages.scrollHeight;
    }
    flushScheduled = false;
  };
  
  const cleanupStream = window.api.onStreamOutput((data) => {
    if (data?.target === 'cases-assistant') return;
    if (data.type === 'claude-stream' && data.text) {
      if (!streamingStarted) {
        streamingStarted = true;
        thinkingText.textContent = 'Generating response...';
        streamingTextDiv.style.display = 'block';
      }
      
      // Buffer the text and flush periodically to avoid UI thrashing
      streamBuffer += data.text;
      if (!flushScheduled) {
        flushScheduled = true;
        requestAnimationFrame(flushStreamBuffer);
      }
    }
  });
  
  try {
    // Send question with full conversation history
    const result = await window.api.troubleshoot(question, conversationHistory);
    
    // Clean up listeners
    cleanupToolCall();
    cleanupStream();
    flushStreamBuffer(); // Flush any remaining buffered text
    
    // Remove thinking indicator
    chatMessages.removeChild(thinkingDiv);
    
    if (result.success) {
      // Update conversation history from response
      if (result.history && result.history.length > 0) {
        conversationHistory = result.history;
      } else {
        // Fallback: manually update history
        conversationHistory.push({ role: 'user', content: question });
        conversationHistory.push({ role: 'assistant', content: result.response });
      }
      
      addMessage(result.response, false);
      
      // Show history size indicator
      console.log(`[Chat] Conversation history: ${conversationHistory.length} messages`);
    } else {
      addMessage(`Error: ${result.error}`, false);
    }
  } catch (error) {
    cleanupToolCall();
    cleanupStream();
    flushStreamBuffer();
    chatMessages.removeChild(thinkingDiv);
    addMessage(`Error: ${error.message}`, false);
  }
}

btnSend.addEventListener('click', sendMessage);
chatInput.addEventListener('keypress', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
});

// Clear chat button
document.getElementById('btn-clear-chat').addEventListener('click', () => {
  clearConversation();
  // Re-add the welcome message
  const welcomeHtml = `
    <div class="message assistant">
      <div class="message-content">
        <p>Conversation cleared. How can I help you?</p>
      </div>
    </div>
  `;
  chatMessages.innerHTML = welcomeHtml;
});

// Show graph button in troubleshoot - shows graph of last mentioned node
document.getElementById('btn-show-graph')?.addEventListener('click', async () => {
  // Try to find node names mentioned in the conversation
  const messages = chatMessages.querySelectorAll('.message-content');
  let foundNode = null;
  
  // Look for node names in the conversation (patterns like AOI names, equipment, etc.)
  const nodePatterns = [
    /\b([A-Z][a-zA-Z0-9_]+_[A-Z][a-zA-Z0-9_]+)\b/g,  // AOI-style names like Motor_Start
    /\b(AOI|UDT|View|Equipment|Tag):\s*([A-Za-z0-9_]+)/gi,  // Explicit mentions
  ];
  
  // Search backwards through messages to find most recent node mention
  for (let i = messages.length - 1; i >= 0 && !foundNode; i--) {
    const text = messages[i].textContent || '';
    
    for (const pattern of nodePatterns) {
      const matches = text.matchAll(pattern);
      for (const match of matches) {
        foundNode = match[1] || match[2];
        break;
      }
      if (foundNode) break;
    }
  }
  
  if (foundNode) {
    // Try to determine the type
    openGraphModal(foundNode, null, foundNode);
  } else {
    // No specific node found - show full graph
    // Switch to graph tab
    navButtons.forEach(b => b.classList.remove('active'));
    document.querySelector('[data-tab="graph"]')?.classList.add('active');
    
    tabContents.forEach(tab => tab.classList.remove('active'));
    document.getElementById('tab-graph')?.classList.add('active');
    
    initGraphTab();
  }
});

// ============================================
// Graph Hover Preview (for inline references)
// ============================================

let hoverGraphRenderer = null;
let hoverTimeout = null;
let currentHoverNode = null;

const hoverPreview = document.getElementById('graph-hover-preview');
const hoverCanvas = document.getElementById('graph-hover-canvas');

// Event delegation for graph references in chat
chatMessages?.addEventListener('mouseover', (e) => {
  const ref = e.target.closest('.graph-ref');
  if (!ref) return;
  
  const nodeName = ref.dataset.name;
  const nodeType = ref.dataset.type || null;
  
  if (!nodeName || nodeName === currentHoverNode) return;
  
  // Clear any pending hide
  clearTimeout(hoverTimeout);
  
  // Show preview after small delay
  hoverTimeout = setTimeout(() => {
    showGraphHoverPreview(ref, nodeName, nodeType);
  }, 300);
});

chatMessages?.addEventListener('mouseout', (e) => {
  const ref = e.target.closest('.graph-ref');
  if (!ref) return;
  
  // Hide after delay (allow moving to the preview)
  hoverTimeout = setTimeout(() => {
    hideGraphHoverPreview();
  }, 200);
});

// Keep preview open when hovering over it
hoverPreview?.addEventListener('mouseover', () => {
  clearTimeout(hoverTimeout);
});

hoverPreview?.addEventListener('mouseout', () => {
  hoverTimeout = setTimeout(() => {
    hideGraphHoverPreview();
  }, 200);
});

// Click on preview opens full modal
hoverPreview?.addEventListener('click', () => {
  if (currentHoverNode) {
    hideGraphHoverPreview();
    openGraphModal(currentHoverNode, null, currentHoverNode);
  }
});

async function showGraphHoverPreview(refElement, nodeName, nodeType) {
  if (!hoverPreview || !hoverCanvas) return;
  
  currentHoverNode = nodeName;
  
  // Position the preview near the reference
  const rect = refElement.getBoundingClientRect();
  const previewWidth = 320;
  const previewHeight = 280;
  
  let left = rect.left;
  let top = rect.bottom + 8;
  
  // Adjust if off-screen
  if (left + previewWidth > window.innerWidth) {
    left = window.innerWidth - previewWidth - 16;
  }
  if (top + previewHeight > window.innerHeight) {
    top = rect.top - previewHeight - 8;
  }
  
  hoverPreview.style.left = `${left}px`;
  hoverPreview.style.top = `${top}px`;
  
  // Update header
  document.querySelector('.graph-hover-title').textContent = nodeName;
  document.querySelector('.graph-hover-type').textContent = nodeType || 'Node';
  
  // Show loading state
  hoverCanvas.innerHTML = '<div class="graph-hover-loading">Loading graph...</div>';
  hoverPreview.classList.add('active');
  
  // Load graph data
  try {
    const result = await window.api.graphNeighbors({
      nodeId: nodeName,
      nodeType: nodeType,
      hops: 1,
      maxNodes: 15
    });
    
    if (!result.success || !result.nodes || result.nodes.length === 0) {
      // Try search
      const searchResult = await window.api.graphSearch(nodeName, { limit: 5 });
      if (searchResult.success && searchResult.nodes && searchResult.nodes.length > 0) {
        const firstMatch = searchResult.nodes[0];
        const neighborResult = await window.api.graphNeighbors({
          nodeId: firstMatch.label,
          nodeType: firstMatch.type,
          hops: 1,
          maxNodes: 15
        });
        
        if (neighborResult.success && neighborResult.nodes?.length > 0) {
          renderHoverGraph(neighborResult);
          return;
        }
      }
      
      hoverCanvas.innerHTML = '<div class="graph-hover-error">Node not found in database.<br>It may not be ingested yet.</div>';
      return;
    }
    
    renderHoverGraph(result);
    
  } catch (error) {
    console.error('Hover preview error:', error);
    hoverCanvas.innerHTML = '<div class="graph-hover-error">Error loading graph</div>';
  }
}

function renderHoverGraph(data) {
  // Clear previous
  hoverCanvas.innerHTML = '';
  
  // Destroy old renderer
  if (hoverGraphRenderer) {
    hoverGraphRenderer.destroy();
    hoverGraphRenderer = null;
  }
  
  // Create mini graph renderer
  hoverGraphRenderer = new GraphRenderer(hoverCanvas, {
    editable: false,
    layout: 'force'
  });
  
  hoverGraphRenderer.loadData(data);
  
  // Fit after layout
  setTimeout(() => {
    if (hoverGraphRenderer) {
      hoverGraphRenderer.fit();
    }
  }, 300);
}

function hideGraphHoverPreview() {
  if (hoverPreview) {
    hoverPreview.classList.remove('active');
  }
  currentHoverNode = null;
}

// ============================================
// Database Tab
// ============================================

const statsDisplay = document.getElementById('stats-display');

async function updateStats() {
  try {
    const result = await window.api.getStats();
    if (result.success) {
      // Clean and format the output
      const cleaned = result.output
        .split('\n')
        .filter(line => !line.includes('GqlStatusObject') && !line.includes('Received notification'))
        .join('\n');
      
      // Add project-specific stats if projects exist
      let projectStats = '';
      if (browseState.projects && browseState.projects.length > 0) {
        projectStats = '\n\n--- Projects ---\n';
        for (const p of browseState.projects) {
          const parentInfo = p.parent ? ` (from ${p.parent})` : '';
          const inheritableInfo = p.inheritable ? ' [lib]' : '';
          projectStats += `${p.name}${parentInfo}${inheritableInfo}\n`;
        }
      }
      
      statsDisplay.textContent = cleaned + projectStats;
      
      // Update Neo4j status
      updateNeo4jStatus(true);
    } else {
      statsDisplay.textContent = `Error: ${result.error}`;
      updateNeo4jStatus(false);
    }
  } catch (error) {
    statsDisplay.textContent = `Error: ${error.message}`;
    updateNeo4jStatus(false);
  }
}

function updateNeo4jStatus(connected) {
  const statusEl = document.getElementById('neo4j-status');
  const dot = statusEl.querySelector('.status-dot');
  const text = statusEl.querySelector('span:last-child');
  
  if (connected) {
    dot.className = 'status-dot connected';
    text.textContent = 'Neo4j Connected';
  } else {
    dot.className = 'status-dot error';
    text.textContent = 'Neo4j Disconnected';
  }
}

document.getElementById('btn-get-stats').addEventListener('click', () => {
  updateStats();
});

// Clear Database
document.getElementById('btn-clear-db').addEventListener('click', async () => {
  const confirmed = confirm('WARNING: This will DELETE ALL DATA from the ontology database.\n\nAre you sure you want to continue?');
  if (!confirmed) return;
  
  showLoading('Clearing database...');
  
  try {
    const result = await window.api.clearDatabase();
    if (result.success) {
      alert('Database cleared successfully!');
    } else {
      alert(`Error: ${result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
  
  hideLoading();
  updateStats();
  // Refresh browse tab data after clearing
  await loadProjects();
  await loadGatewayResources();
});

// Initialize Schema
document.getElementById('btn-init-db').addEventListener('click', async () => {
  showLoading('Initializing database schema...');
  
  try {
    const result = await window.api.initDatabase();
    if (result.success) {
      alert('Database schema initialized!');
    } else {
      alert(`Error: ${result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
  
  hideLoading();
  updateStats();
});

// Generate Visualization
document.getElementById('btn-generate-viz').addEventListener('click', async () => {
  showLoading('Generating visualization...');
  
  try {
    const result = await window.api.generateViz();
    if (result.success) {
      alert(`Visualization generated!\n\nOpening: ${result.path}`);
      // Note: In a full implementation, we'd shell.openPath here
    } else {
      alert(`Error: ${result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
  
  hideLoading();
});

// Save Database
document.getElementById('btn-save-db').addEventListener('click', async () => {
  showLoading('Saving database...');
  
  try {
    const result = await window.api.saveDatabase();
    if (result.success) {
      alert(`Database saved!\n\nFile: ${result.path}`);
    } else if (result.error !== 'Save cancelled') {
      alert(`Error: ${result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
  
  hideLoading();
});

// Load Database
document.getElementById('btn-load-db').addEventListener('click', async () => {
  const confirmed = confirm('WARNING: This will REPLACE all data in the database with the backup file.\n\nAre you sure you want to continue?');
  if (!confirmed) return;
  
  showLoading('Loading database...');
  
  try {
    const result = await window.api.loadDatabase();
    if (result.success) {
      alert(`Database loaded!\n\nFile: ${result.path}`);
      updateStats();
    } else if (result.error !== 'Load cancelled') {
      alert(`Error: ${result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
  
  hideLoading();
});

// ============================================
// Browse Tab - Projects and Resources
// ============================================

// State for Browse tab
let browseState = {
  projects: [],
  currentSubTab: 'gateway',
  lastIgnitionFile: null,
  scriptLibraryPath: null,
  namedQueriesPath: null
};

// Sub-tab navigation
function initBrowseSubTabs() {
  const subTabContainer = document.getElementById('browse-sub-tabs');
  
  subTabContainer.addEventListener('click', (e) => {
    const subTab = e.target.closest('.sub-tab');
    if (!subTab) return;
    
    const subtabId = subTab.dataset.subtab;
    if (!subtabId) return;
    
    // Update active state
    subTabContainer.querySelectorAll('.sub-tab').forEach(t => t.classList.remove('active'));
    subTab.classList.add('active');
    
    // Show corresponding content
    document.querySelectorAll('#tab-browse .sub-tab-content').forEach(content => {
      content.classList.remove('active');
    });
    
    if (subtabId === 'gateway') {
      document.getElementById('subtab-gateway').classList.add('active');
    } else if (subtabId.startsWith('tia:')) {
      // TIA project sub-tab
      const tiaProjectName = subtabId.replace('tia:', '');
      const tiaContent = document.querySelector(`#tab-browse .sub-tab-content[data-tia-project="${tiaProjectName}"]`);
      if (tiaContent) tiaContent.classList.add('active');
    } else {
      const projectContent = document.querySelector(`#tab-browse .sub-tab-content[data-project="${subtabId}"]`);
      if (projectContent) projectContent.classList.add('active');
    }
    
    browseState.currentSubTab = subtabId;
  });
}

// Load and display projects
async function loadProjects() {
  try {
    const result = await window.api.getProjects();
    if (result.success) {
      browseState.projects = result.projects || [];
      renderProjectTabs();
      renderProjectTree();
    }
  } catch (error) {
    console.error('Failed to load projects:', error);
  }
}

// Render project sub-tabs
function renderProjectTabs() {
  const container = document.getElementById('browse-sub-tabs');
  
  // Keep Gateway tab, remove others
  const gatewayTab = container.querySelector('[data-subtab="gateway"]');
  container.innerHTML = '';
  container.appendChild(gatewayTab);
  
  // Add project tabs
  for (const project of browseState.projects) {
    const tab = document.createElement('button');
    tab.className = 'sub-tab';
    tab.dataset.subtab = project.name;
    tab.textContent = project.name;
    
    // Add inheritance indicator as tooltip
    if (project.parent) {
      tab.title = `Inherits from: ${project.parent}`;
    }
    
    container.appendChild(tab);
  }
  
  // Create content sections for each project
  renderProjectContents();
}

// Render project content sections
function renderProjectContents() {
  const template = document.getElementById('project-subtab-template');
  const browseTab = document.getElementById('tab-browse');
  
  // Remove old project contents (keep gateway and template)
  browseTab.querySelectorAll('.sub-tab-content[data-project]:not([data-project=""])').forEach(el => el.remove());
  
  for (const project of browseState.projects) {
    const content = template.content.cloneNode(true);
    const container = content.querySelector('.sub-tab-content');
    
    container.dataset.project = project.name;
    container.querySelector('.project-name').textContent = `Project: ${project.name}`;
    
    if (project.parent) {
      container.querySelector('.project-inheritance').textContent = `Inherits from: ${project.parent}`;
    } else if (project.inheritable) {
      container.querySelector('.project-inheritance').textContent = '(base library)';
    }
    
    // Add enrich button handlers
    const enrichViewsBtn = container.querySelector('.btn-enrich-views');
    const enrichScriptsBtn = container.querySelector('.btn-enrich-scripts');
    const enrichQueriesBtn = container.querySelector('.btn-enrich-queries');
    const enrichEventsBtn = container.querySelector('.btn-enrich-events');
    const enrichComponentsBtn = container.querySelector('.btn-enrich-components');
    
    enrichViewsBtn.addEventListener('click', () => enrichBatch('View', project.name));
    enrichScriptsBtn.addEventListener('click', () => enrichBatch('Script', project.name));
    enrichQueriesBtn.addEventListener('click', () => enrichBatch('NamedQuery', project.name));
    enrichEventsBtn.addEventListener('click', () => enrichBatch('GatewayEvent', project.name));
    enrichComponentsBtn.addEventListener('click', () => enrichBatch('ViewComponent', project.name));
    
    browseTab.appendChild(container);
    
    // Load resources for this project
    loadProjectResources(project.name);
  }
}

// Render project discovery tree
function renderProjectTree() {
  const treeContainer = document.getElementById('project-tree');
  if (!treeContainer) return;
  
  if (browseState.projects.length === 0) {
    treeContainer.innerHTML = '<p style="color: var(--text-muted);">No projects found</p>';
    return;
  }
  
  // Build inheritance map
  const childrenMap = {};
  const rootProjects = [];
  
  for (const p of browseState.projects) {
    if (p.parent) {
      if (!childrenMap[p.parent]) childrenMap[p.parent] = [];
      childrenMap[p.parent].push(p);
    } else {
      rootProjects.push(p);
    }
  }
  
  // Render tree
  function renderNode(project, indent = 0) {
    const inheritable = project.inheritable ? ' [inheritable]' : '';
    const prefix = indent > 0 ? '├── ' : '';
    let html = `<div class="project-item" style="padding-left: ${indent * 16}px">`;
    html += `<span class="inherit-line">${prefix}</span>`;
    html += `<span class="project-label">${project.name}</span>`;
    html += `<span class="project-info">${inheritable}</span>`;
    html += '</div>';
    
    const children = childrenMap[project.name] || [];
    for (const child of children) {
      html += renderNode(child, indent + 1);
    }
    
    return html;
  }
  
  let html = '';
  for (const root of rootProjects) {
    html += renderNode(root);
  }
  
  treeContainer.innerHTML = html;
}

// Load gateway resources
async function loadGatewayResources() {
  try {
    const result = await window.api.getGatewayResources();
    if (!result.success) return;
    
    const resources = result.resources;
    
    // Update counts
    document.getElementById('tag-count').textContent = `(${resources.tags?.length || 0})`;
    document.getElementById('udt-count').textContent = `(${resources.udts?.length || 0})`;
    document.getElementById('aoi-count').textContent = `(${resources.aois?.length || 0})`;
    
    // Render lists
    renderResourceList('tag-list', resources.tags || [], 'name');
    renderResourceList('udt-list', resources.udts || [], 'name');
    renderResourceList('aoi-list', resources.aois || [], 'name');
    
  } catch (error) {
    console.error('Failed to load gateway resources:', error);
  }
}

// Load project-specific resources
async function loadProjectResources(projectName) {
  try {
    const result = await window.api.getProjectResources(projectName);
    if (!result.success) return;
    
    const resources = result.resources;
    const container = document.querySelector(`#tab-browse .sub-tab-content[data-project="${projectName}"]`);
    if (!container) return;
    
    // Update counts
    container.querySelector('.section-views .section-count').textContent = `(${resources.views?.length || 0})`;
    container.querySelector('.section-scripts .section-count').textContent = `(${resources.scripts?.length || 0})`;
    container.querySelector('.section-queries .section-count').textContent = `(${resources.queries?.length || 0})`;
    container.querySelector('.section-events .section-count').textContent = `(${resources.events?.length || 0})`;
    container.querySelector('.section-components .section-count').textContent = `(${resources.components?.length || 0})`;
    
    // Render lists
    renderViewList(container.querySelector('.view-list'), resources.views || []);
    renderResourceList(container.querySelector('.script-list'), resources.scripts || [], 'path');
    renderResourceList(container.querySelector('.query-list'), resources.queries || [], 'name');
    renderResourceList(container.querySelector('.event-list'), resources.events || [], 'name');
    renderComponentList(container.querySelector('.component-list'), resources.components || []);
    
  } catch (error) {
    console.error(`Failed to load resources for ${projectName}:`, error);
  }
}

// Render component list with view grouping info
function renderComponentList(container, items) {
  if (!container) return;
  
  container.innerHTML = '';
  
  // Group by view for better display
  const byView = {};
  for (const item of items) {
    const viewName = item.view_name || 'Unknown';
    if (!byView[viewName]) byView[viewName] = [];
    byView[viewName].push(item);
  }
  
  // Show summary by view
  for (const [viewName, components] of Object.entries(byView).slice(0, 20)) {
    const el = document.createElement('div');
    el.className = 'resource-item';
    
    // Count complete vs pending
    const complete = components.filter(c => c.status === 'complete').length;
    if (complete === components.length) {
      el.classList.add('complete');
    } else {
      el.classList.add('pending');
    }
    
    // Show view name with component count
    const shortName = viewName.split('/').pop();
    el.textContent = `${shortName} (${components.length})`;
    el.title = `${viewName}: ${components.length} components, ${complete} enriched`;
    container.appendChild(el);
  }
  
  if (Object.keys(byView).length > 20) {
    const more = document.createElement('div');
    more.className = 'resource-item';
    more.style.fontStyle = 'italic';
    more.textContent = `... and ${Object.keys(byView).length - 20} more views`;
    container.appendChild(more);
  }
  
  if (items.length === 0) {
    const empty = document.createElement('div');
    empty.style.color = 'var(--text-muted)';
    empty.style.fontStyle = 'italic';
    empty.style.padding = '12px';
    empty.textContent = 'No components found';
    container.appendChild(empty);
  }
}

// Render a list of resources
function renderResourceList(containerOrId, items, labelField) {
  const container = typeof containerOrId === 'string' 
    ? document.getElementById(containerOrId) 
    : containerOrId;
  if (!container) return;
  
  container.innerHTML = '';
  
  for (const item of items.slice(0, 50)) { // Limit to 50 for performance
    const el = document.createElement('div');
    el.className = 'resource-item';
    
    // Add status class
    if (item.status === 'complete') {
      el.classList.add('complete');
    } else {
      el.classList.add('pending');
    }
    
    // Create name span
    const nameSpan = document.createElement('span');
    nameSpan.textContent = item[labelField] || item.name || 'Unknown';
    el.appendChild(nameSpan);
    
    // Add graph button
    const graphBtn = document.createElement('button');
    graphBtn.className = 'btn btn-sm btn-ghost graph-btn';
    graphBtn.textContent = '⤢';
    graphBtn.title = 'View in graph';
    graphBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      const nodeName = item[labelField] || item.name;
      const nodeType = item.type || guessNodeType(containerOrId);
      openGraphModal(nodeName, nodeType, nodeName);
    });
    el.appendChild(graphBtn);
    
    container.appendChild(el);
  }
  
  if (items.length > 50) {
    const more = document.createElement('div');
    more.className = 'resource-item';
    more.style.fontStyle = 'italic';
    more.textContent = `... and ${items.length - 50} more`;
    container.appendChild(more);
  }
}

// Guess node type from container ID
function guessNodeType(containerId) {
  if (typeof containerId !== 'string') return 'Unknown';
  if (containerId.includes('tag')) return 'ScadaTag';
  if (containerId.includes('udt')) return 'UDT';
  if (containerId.includes('aoi')) return 'AOI';
  if (containerId.includes('view')) return 'View';
  if (containerId.includes('script')) return 'Script';
  if (containerId.includes('query')) return 'NamedQuery';
  if (containerId.includes('event')) return 'GatewayEvent';
  if (containerId.includes('component')) return 'ViewComponent';
  return 'Unknown';
}

// Render view list with component counts
function renderViewList(container, views) {
  if (!container) return;
  
  container.innerHTML = '';
  
  for (const view of views.slice(0, 100)) { // Limit to 100 views
    const el = document.createElement('div');
    el.className = 'resource-item view-item';
    
    // Add status class based on view status
    if (view.status === 'complete') {
      el.classList.add('complete');
    } else {
      el.classList.add('pending');
    }
    
    // Create view name span
    const nameSpan = document.createElement('span');
    nameSpan.className = 'view-name';
    nameSpan.textContent = view.name || 'Unknown';
    el.appendChild(nameSpan);
    
    // Create component count badge if view has components
    if (view.component_count > 0) {
      const badge = document.createElement('span');
      badge.className = 'component-badge';
      
      if (view.enriched_count === view.component_count) {
        badge.classList.add('all-enriched');
      } else if (view.enriched_count > 0) {
        badge.classList.add('partial');
      }
      
      badge.textContent = `${view.enriched_count}/${view.component_count}`;
      badge.title = `${view.enriched_count} of ${view.component_count} components enriched`;
      el.appendChild(badge);
    }
    
    // Add graph button
    const graphBtn = document.createElement('button');
    graphBtn.className = 'btn btn-sm btn-ghost graph-btn';
    graphBtn.textContent = '⤢';
    graphBtn.title = 'View in graph';
    graphBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      openGraphModal(view.name, 'View', view.name);
    });
    el.appendChild(graphBtn);
    
    container.appendChild(el);
  }
  
  if (views.length > 100) {
    const more = document.createElement('div');
    more.className = 'resource-item';
    more.style.fontStyle = 'italic';
    more.textContent = `... and ${views.length - 100} more`;
    container.appendChild(more);
  }
}

// Enrich a batch of items
// Get enrichment log element
function getEnrichmentLog() {
  return document.getElementById('enrichment-log');
}

function appendEnrichmentLog(text, clear = false) {
  const log = getEnrichmentLog();
  if (!log) return;
  if (clear) {
    log.textContent = '';
  }
  log.textContent += text;
  log.scrollTop = log.scrollHeight;
}

// Track active enrichment stream
let activeEnrichmentStreamId = null;

// Set up enrichment stream listeners
window.api.onStreamOutput((data) => {
  // Only process our enrichment stream
  if (data.streamId && data.streamId.startsWith('enrich-')) {
    appendEnrichmentLog(data.text + '\n');
  }
});

window.api.onStreamComplete((data) => {
  if (data.streamId && data.streamId.startsWith('enrich-')) {
    if (!data.success) {
      appendEnrichmentLog(`\n[FAILED] Enrichment failed\n`);
    } else {
      appendEnrichmentLog(`\n[OK] Enrichment complete\n`);
    }
  }
});

async function enrichBatch(itemType, projectName = null) {
  if (!browseState.lastIgnitionFile) {
    alert('Please ingest an Ignition backup first (needed for enrichment context)');
    return;
  }
  
  // Get batch size from input
  const batchSizeInput = document.getElementById('enrichment-batch-size');
  const batchSize = parseInt(batchSizeInput?.value || '5', 10);
  
  const btn = event.target;
  const originalText = btn.textContent;
  btn.textContent = 'Enriching...';
  btn.disabled = true;
  
  // Log the enrichment start
  const projInfo = projectName ? `project ${projectName}` : 'gateway';
  appendEnrichmentLog(`\n[START] Enriching ${batchSize} ${itemType} items for ${projInfo}...\n`);
  
  try {
    const result = await window.api.enrichBatch({
      project: projectName,
      itemType: itemType,
      batchSize: batchSize,
      inputFile: browseState.lastIgnitionFile
    });
    
    activeEnrichmentStreamId = result.streamId;
    
    if (result.success) {
      // Logs will appear via streaming
      // Refresh the resources after a brief delay
      setTimeout(() => {
        if (projectName) {
          loadProjectResources(projectName);
        } else {
          loadGatewayResources();
        }
      }, 1000);
    } else {
      appendEnrichmentLog(`[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendEnrichmentLog(`[ERROR] ${error.message}\n`);
    console.error('Enrichment failed:', error);
  } finally {
    btn.textContent = originalText;
    btn.disabled = false;
  }
  
  btn.textContent = originalText;
  btn.disabled = false;
}

// Gateway enrichment buttons
document.getElementById('btn-enrich-tags')?.addEventListener('click', () => enrichBatch('ScadaTag'));
document.getElementById('btn-enrich-udts')?.addEventListener('click', () => enrichBatch('UDT'));
document.getElementById('btn-enrich-aois')?.addEventListener('click', () => enrichBatch('AOI'));

// ============================================
// TIA Portal Browse (Siemens projects)
// ============================================

let tiaState = {
  projects: [],
};

// Load TIA Portal projects
async function loadTiaProjects() {
  try {
    const result = await window.api.getTiaProjects();
    if (result.success) {
      tiaState.projects = result.projects || [];
      renderTiaProjectTabs();
    }
  } catch (error) {
    console.error('Failed to load TIA projects:', error);
  }
}

// Render TIA project sub-tabs alongside Ignition projects
function renderTiaProjectTabs() {
  const container = document.getElementById('browse-sub-tabs');
  
  // Remove old TIA tabs (keep gateway + Ignition project tabs)
  container.querySelectorAll('.sub-tab[data-tia]').forEach(el => el.remove());
  
  // Add TIA project tabs
  for (const project of tiaState.projects) {
    const tab = document.createElement('button');
    tab.className = 'sub-tab';
    tab.dataset.subtab = `tia:${project.name}`;
    tab.dataset.tia = 'true';
    tab.innerHTML = `<span style="color: #0288D1;">▪</span> ${project.name}`;
    tab.title = `Siemens TIA: ${project.plc_count || 0} PLCs, ${project.hmi_count || 0} HMIs`;
    container.appendChild(tab);
  }
  
  // Create content sections for each TIA project
  renderTiaProjectContents();
}

// Render TIA project content sections
function renderTiaProjectContents() {
  const template = document.getElementById('tia-project-subtab-template');
  const browseTab = document.getElementById('tab-browse');
  
  // Remove old TIA project contents
  browseTab.querySelectorAll('.sub-tab-content[data-tia-project]').forEach(el => {
    if (el.dataset.tiaProject) el.remove();
  });
  
  for (const project of tiaState.projects) {
    const content = template.content.cloneNode(true);
    const container = content.querySelector('.sub-tab-content');
    
    container.dataset.tiaProject = project.name;
    container.querySelector('.project-name').textContent = `TIA Project: ${project.name}`;
    
    // Add enrich button handlers for Siemens types
    const enrichPlcBlocksBtn = container.querySelector('.btn-enrich-plc-blocks');
    const enrichPlcTagsBtn = container.querySelector('.btn-enrich-plc-tags');
    const enrichHmiScriptsBtn = container.querySelector('.btn-enrich-hmi-scripts');
    const enrichHmiAlarmsBtn = container.querySelector('.btn-enrich-hmi-alarms');
    const enrichHmiScreensBtn = container.querySelector('.btn-enrich-hmi-screens');
    
    enrichPlcBlocksBtn?.addEventListener('click', () => enrichTiaBatch('AOI'));
    enrichPlcTagsBtn?.addEventListener('click', () => enrichTiaBatch('PLCTag'));
    enrichHmiScriptsBtn?.addEventListener('click', () => enrichTiaBatch('HMIScript'));
    enrichHmiAlarmsBtn?.addEventListener('click', () => enrichTiaBatch('HMIAlarm'));
    enrichHmiScreensBtn?.addEventListener('click', () => enrichTiaBatch('HMIScreen'));
    
    browseTab.appendChild(container);
    
    // Load resources for this TIA project
    loadTiaProjectResources(project.name);
  }
}

// Load TIA project resources
async function loadTiaProjectResources(projectName) {
  try {
    const result = await window.api.getTiaProjectResources(projectName);
    if (!result.success) {
      console.warn(`TIA resources for ${projectName}: API returned error:`, result.error);
      return;
    }
    
    const resources = result.resources;
    const container = document.querySelector(`#tab-browse .sub-tab-content[data-tia-project="${projectName}"]`);
    if (!container) return;
    
    // Update counts
    container.querySelector('.section-plc-blocks .section-count').textContent = `(${resources.plc_blocks?.length || 0})`;
    container.querySelector('.section-plc-tags .section-count').textContent = `(${resources.plc_tags?.length || 0})`;
    container.querySelector('.section-plc-types .section-count').textContent = `(${resources.plc_types?.length || 0})`;
    container.querySelector('.section-hmi-scripts .section-count').textContent = `(${resources.hmi_scripts?.length || 0})`;
    container.querySelector('.section-hmi-alarms .section-count').textContent = `(${resources.hmi_alarms?.length || 0})`;
    container.querySelector('.section-hmi-screens .section-count').textContent = `(${resources.hmi_screens?.length || 0})`;
    container.querySelector('.section-hmi-connections .section-count').textContent = `(${resources.hmi_connections?.length || 0})`;
    
    // Render resource lists
    renderResourceList(container.querySelector('.plc-blocks-list'), resources.plc_blocks || [], 'name');
    renderTiaTagList(container.querySelector('.plc-tags-list'), resources.plc_tags || []);
    renderResourceList(container.querySelector('.plc-types-list'), resources.plc_types || [], 'name');
    renderTiaScriptList(container.querySelector('.hmi-scripts-list'), resources.hmi_scripts || []);
    renderTiaAlarmList(container.querySelector('.hmi-alarms-list'), resources.hmi_alarms || []);
    renderResourceList(container.querySelector('.hmi-screens-list'), resources.hmi_screens || [], 'name');
    renderTiaConnectionList(container.querySelector('.hmi-connections-list'), resources.hmi_connections || []);
    
  } catch (error) {
    console.error(`Failed to load TIA resources for ${projectName}:`, error);
  }
}

// Render TIA PLC tag list with data type and address
function renderTiaTagList(container, tags) {
  if (!container) return;
  container.innerHTML = '';
  
  for (const tag of tags.slice(0, 100)) {
    const el = document.createElement('div');
    el.className = 'resource-item';
    if (tag.status === 'complete') el.classList.add('complete');
    else el.classList.add('pending');
    
    const addr = tag.logical_address ? ` @ ${tag.logical_address}` : '';
    el.textContent = `${tag.name} (${tag.data_type}${addr})`;
    el.title = `Table: ${tag.table_name || '?'} | Device: ${tag.device || '?'}${tag.comment ? '\n' + tag.comment : ''}`;
    container.appendChild(el);
  }
  
  if (tags.length > 100) {
    const more = document.createElement('div');
    more.className = 'resource-item';
    more.style.fontStyle = 'italic';
    more.textContent = `... and ${tags.length - 100} more`;
    container.appendChild(more);
  }
}

// Render TIA HMI script list with function count
function renderTiaScriptList(container, scripts) {
  if (!container) return;
  container.innerHTML = '';
  
  for (const script of scripts) {
    const el = document.createElement('div');
    el.className = 'resource-item';
    if (script.status === 'complete') el.classList.add('complete');
    else el.classList.add('pending');
    
    const funcCount = script.functions?.length || 0;
    el.textContent = `${script.name} (${funcCount} functions)`;
    el.title = `Functions: ${(script.functions || []).slice(0, 10).join(', ')}`;
    container.appendChild(el);
  }
}

// Render TIA HMI alarm list with type
function renderTiaAlarmList(container, alarms) {
  if (!container) return;
  container.innerHTML = '';
  
  for (const alarm of alarms.slice(0, 100)) {
    const el = document.createElement('div');
    el.className = 'resource-item';
    if (alarm.status === 'complete') el.classList.add('complete');
    else el.classList.add('pending');
    
    const typeIcon = alarm.alarm_type === 'Analog' ? '📊' : '🔘';
    el.textContent = `${typeIcon} ${alarm.name} [${alarm.alarm_class || 'No class'}]`;
    el.title = `Type: ${alarm.alarm_type} | Device: ${alarm.device || '?'}`;
    container.appendChild(el);
  }
  
  if (alarms.length > 100) {
    const more = document.createElement('div');
    more.className = 'resource-item';
    more.style.fontStyle = 'italic';
    more.textContent = `... and ${alarms.length - 100} more`;
    container.appendChild(more);
  }
}

// Render TIA HMI connection list with partner info
function renderTiaConnectionList(container, connections) {
  if (!container) return;
  container.innerHTML = '';
  
  for (const conn of connections) {
    const el = document.createElement('div');
    el.className = 'resource-item';
    el.textContent = `${conn.name} → ${conn.partner || '?'} (${conn.driver || 'Unknown'})`;
    el.title = `Device: ${conn.device || '?'}`;
    container.appendChild(el);
  }
}

// Enrich TIA items (no backup file needed)
async function enrichTiaBatch(itemType) {
  const batchSizeInput = document.getElementById('enrichment-batch-size');
  const batchSize = parseInt(batchSizeInput?.value || '5', 10);
  
  const btn = event.target;
  const originalText = btn.textContent;
  btn.textContent = 'Enriching...';
  btn.disabled = true;
  
  appendEnrichmentLog(`\n[START] Enriching ${batchSize} ${itemType} items (Siemens TIA)...\n`);
  
  try {
    const result = await window.api.enrichTiaBatch({
      itemType: itemType,
      batchSize: batchSize,
    });
    
    if (result.success) {
      // Refresh TIA resources after enrichment
      setTimeout(() => {
        for (const project of tiaState.projects) {
          loadTiaProjectResources(project.name);
        }
      }, 1000);
    } else {
      appendEnrichmentLog(`[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendEnrichmentLog(`[ERROR] ${error.message}\n`);
    console.error('TIA enrichment failed:', error);
  } finally {
    btn.textContent = originalText;
    btn.disabled = false;
  }
}

// Go to Browse tab button
document.getElementById('btn-goto-browse')?.addEventListener('click', () => {
  // Switch to browse tab
  navButtons.forEach(b => b.classList.remove('active'));
  document.querySelector('[data-tab="browse"]').classList.add('active');
  
  tabContents.forEach(tab => tab.classList.remove('active'));
  document.getElementById('tab-browse').classList.add('active');
});

// Directory picker for script library
document.getElementById('btn-pick-script-library')?.addEventListener('click', async () => {
  const dirPath = await window.api.selectDirectory();
  if (dirPath) {
    browseState.scriptLibraryPath = dirPath;
    const displayEl = document.getElementById('script-library-path');
    if (displayEl) {
      displayEl.textContent = dirPath.split(/[/\\]/).pop();
      displayEl.title = dirPath;
      displayEl.classList.add('loaded');
    }
  }
});

// Directory picker for named queries
document.getElementById('btn-pick-named-queries')?.addEventListener('click', async () => {
  const dirPath = await window.api.selectDirectory();
  if (dirPath) {
    browseState.namedQueriesPath = dirPath;
    const displayEl = document.getElementById('named-queries-path');
    if (displayEl) {
      displayEl.textContent = dirPath.split(/[/\\]/).pop();
      displayEl.title = dirPath;
      displayEl.classList.add('loaded');
    }
  }
});

// Ignition Ingest - with project tracking
document.getElementById('btn-ingest-ignition').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  browseState.lastIgnitionFile = filePath;
  
  // Update the enrichment file display
  const fileNameSpan = document.getElementById('enrichment-file-name');
  if (fileNameSpan) {
    const fileName = filePath.split(/[/\\]/).pop();
    fileNameSpan.textContent = fileName;
    fileNameSpan.classList.add('loaded');
    fileNameSpan.title = filePath;
  }
  
  // Don't use loading overlay - we want to see streaming output
  appendOutput(`\n[INGEST] ${filePath}\n`, false);
  if (browseState.scriptLibraryPath) {
    appendOutput(`[INFO] Script library: ${browseState.scriptLibraryPath}\n`);
  }
  if (browseState.namedQueriesPath) {
    appendOutput(`[INFO] Named queries: ${browseState.namedQueriesPath}\n`);
  }
  appendOutput(`[INFO] Using --skip-ai for initial ingestion (use Browse tab to enrich)\n`);
  
  try {
    const result = await window.api.ingestIgnition({
      filePath,
      scriptLibraryPath: browseState.scriptLibraryPath,
      namedQueriesPath: browseState.namedQueriesPath
    });
    if (result.success) {
      // Don't re-append result.output since it's already streamed
      // Just show the final completion message
      appendOutput('\n[OK] Ignition ingestion complete!\n');
      
      // Load projects and show discovery panel
      await loadProjects();
      
      if (browseState.projects.length > 0) {
        document.getElementById('project-discovery')?.classList.remove('hidden');
        appendOutput(`[INFO] Discovered ${browseState.projects.length} projects - see Browse tab\n`);
      }
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
});

// Workbench Ingest - for Axilon Workbench project.json exports
document.getElementById('btn-ingest-workbench').addEventListener('click', async () => {
  const folderPath = await window.api.selectDirectory();
  
  if (!folderPath) return;
  
  // Track the workbench folder for enrichment (use project.json path)
  const projectJsonPath = folderPath + (folderPath.endsWith('/') || folderPath.endsWith('\\') ? '' : '/') + 'project.json';
  browseState.lastIgnitionFile = projectJsonPath;
  
  // Update the enrichment file display
  const fileNameSpan = document.getElementById('enrichment-file-name');
  if (fileNameSpan) {
    fileNameSpan.textContent = 'project.json (workbench)';
    fileNameSpan.classList.add('loaded');
    fileNameSpan.title = projectJsonPath;
  }
  
  // Don't use loading overlay - we want to see streaming output
  appendOutput(`\n[INGEST WORKBENCH] ${folderPath}\n`, false);
  appendOutput(`[INFO] Using --skip-ai for initial ingestion (use Browse tab to enrich)\n`);
  
  try {
    const result = await window.api.ingestWorkbench(folderPath);
    if (result.success) {
      appendOutput('\n[OK] Workbench ingestion complete!\n');
      
      // Load projects and show discovery panel
      await loadProjects();
      
      if (browseState.projects.length > 0) {
        document.getElementById('project-discovery')?.classList.remove('hidden');
        appendOutput(`[INFO] Discovered ${browseState.projects.length} projects - see Browse tab\n`);
      }
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
});

// ============================================
// Streaming Event Listeners
// ============================================

// Listen for streaming output from Python scripts
window.api.onStreamOutput((data) => {
  if (['cases', 'cases-assistant'].includes(data?.target)) return;
  if (data.type === 'debug') {
    // Show debug lines in output panel with special styling
    appendOutput(`${data.text}\n`);
  } else if (data.type === 'output') {
    appendOutput(`${data.text}\n`);
  } else if (data.type === 'stderr') {
    // Filter out common noise from stderr
    const text = data.text;
    if (!text.includes('GqlStatusObject') && !text.includes('Received notification')) {
      appendOutput(`${text}`);
    }
  }
});

// Listen for tool calls
window.api.onToolCall((data) => {
  if (['cases', 'cases-assistant'].includes(data?.target)) return;
  appendOutput(`[TOOL] ${data.tool}\n`);
});

// Listen for stream completion
window.api.onStreamComplete((data) => {
  if (['cases', 'cases-assistant'].includes(data?.target)) return;
  if (data.success) {
    appendOutput('\n[OK] Operation complete!\n');
  }
});

// ============================================
// Graph Tab
// ============================================

let graphRenderer = null;
let modalGraphRenderer = null;
let graphState = {
  loaded: false,
  currentNode: null,
  modalNode: null,
  relSource: null,  // For relationship creation
  relTarget: null   // For relationship creation
};

// Initialize graph tab when it becomes active
function initGraphTab() {
  if (graphState.loaded) return;
  
  const container = document.getElementById('graph-container');
  if (!container) return;
  
  // Show loading
  const loading = document.getElementById('graph-loading');
  if (loading) loading.classList.add('active');
  
  // Create graph renderer (editable mode)
  graphRenderer = new GraphRenderer(container, {
    editable: true,
    layout: 'hierarchical',
    onNodeSelect: onGraphNodeSelect,
    onEdgeSelect: onGraphEdgeSelect
  });
  
  // Set up double-click handler for expansion
  graphRenderer.onNodeDoubleClick = (nodeData) => {
    loadNodeNeighbors(nodeData.fullLabel || nodeData.label, nodeData.type);
  };
  
  // Load graph data
  loadGraphData();
  
  graphState.loaded = true;
}

// Load graph data from backend
async function loadGraphData() {
  const loading = document.getElementById('graph-loading');
  if (loading) loading.classList.add('active');
  
  try {
    const result = await window.api.graphLoad({});
    
    if (result.success && graphRenderer) {
      graphRenderer.loadData(result);
      if (loading) loading.classList.remove('active');
    } else {
      console.error('Failed to load graph:', result.error);
      if (loading) {
        loading.innerHTML = `<p>Error loading graph: ${result.error}</p>`;
      }
    }
  } catch (error) {
    console.error('Failed to load graph:', error);
    if (loading) {
      loading.innerHTML = `<p>Error: ${error.message}</p>`;
    }
  }
}

// Load neighbors for a node
async function loadNodeNeighbors(nodeName, nodeType) {
  const loading = document.getElementById('graph-loading');
  if (loading) loading.classList.add('active');
  
  try {
    const result = await window.api.graphNeighbors({
      nodeId: nodeName,
      nodeType: nodeType,
      hops: 2,
      maxNodes: 50
    });
    
    if (result.success && graphRenderer) {
      graphRenderer.loadData(result);
      if (loading) loading.classList.remove('active');
    } else {
      console.error('Failed to load neighbors:', result.error);
      if (loading) loading.classList.remove('active');
    }
  } catch (error) {
    console.error('Failed to load neighbors:', error);
    if (loading) loading.classList.remove('active');
  }
}

// Handle node selection in graph
function onGraphNodeSelect(nodeData, event) {
  graphState.currentNode = nodeData;
  updateNodeDetailsPanel(nodeData);
  
  // Show edit section
  const editSection = document.getElementById('edit-section');
  if (editSection) editSection.style.display = 'block';
  
  // Show relationship section
  const relSection = document.getElementById('add-relationship-section');
  if (relSection) relSection.style.display = 'block';
  
  // Check if shift key is held - set as target, otherwise set as source
  const nodeName = nodeData.fullLabel || nodeData.label;
  const isShiftClick = event && event.shiftKey;
  
  if (isShiftClick && graphState.relSource) {
    // Set as target (shift+click)
    graphState.relTarget = { name: nodeName, type: nodeData.type, id: nodeData.id };
    document.getElementById('rel-target').value = nodeName;
  } else {
    // Set as source (normal click)
    graphState.relSource = { name: nodeName, type: nodeData.type, id: nodeData.id };
    document.getElementById('rel-source').value = nodeName;
    // Clear target when selecting new source
    graphState.relTarget = null;
    document.getElementById('rel-target').value = '';
  }
  
  // Populate edit form
  const editName = document.getElementById('edit-name');
  const editType = document.getElementById('edit-type');
  const dynamicProps = document.getElementById('edit-dynamic-props');
  
  if (editName) editName.value = nodeName;
  if (editType) editType.value = nodeData.type;
  
  // Build dynamic property fields
  if (dynamicProps) {
    let propsHtml = '';
    const props = nodeData.properties || {};
    
    // Read-only system properties (not editable)
    const readOnlyProps = [
      'name', 'path', 'project', 'view',
      'query_text', 'script_text', 'props',
      'semantic_status',
      'created_at', 'updated_at', 'analyzed_at',
      'source_file', 'revision', 'vendor'
    ];
    
    // Editable properties (user can modify these)
    const editableProps = ['purpose', 'inferred_purpose', 'type', 'description', 'notes', 'tags', 'category'];
    
    // Show editable properties first
    for (const key of editableProps) {
      if (props[key] !== undefined && props[key] !== null) {
        const value = props[key];
        if (typeof value === 'object') continue;
        
        const displayValue = typeof value === 'string' ? value : JSON.stringify(value);
        propsHtml += `
          <div class="prop-row editable" data-key="${key}">
            <span class="prop-key" title="${key}">${key}</span>
            <textarea class="prop-value" rows="2">${escapeHtml(displayValue)}</textarea>
          </div>
        `;
      }
    }
    
    // Add separator if we have both editable and read-only
    let hasReadOnly = false;
    for (const [key, value] of Object.entries(props)) {
      if (editableProps.includes(key)) continue;
      if (value === null || value === undefined) continue;
      if (typeof value === 'object') continue;
      hasReadOnly = true;
      break;
    }
    
    if (propsHtml && hasReadOnly) {
      propsHtml += '<div class="prop-separator">Read-only</div>';
    }
    
    // Show read-only properties (display only, not in form)
    for (const [key, value] of Object.entries(props)) {
      if (editableProps.includes(key)) continue;
      if (value === null || value === undefined) continue;
      if (typeof value === 'object') continue;
      
      let displayValue = typeof value === 'string' ? value : JSON.stringify(value);
      // Truncate long values
      if (displayValue.length > 100) {
        displayValue = displayValue.substring(0, 100) + '...';
      }
      
      propsHtml += `
        <div class="prop-row readonly" data-key="${key}">
          <span class="prop-key" title="${key}">${key}</span>
          <span class="prop-value-readonly" title="${escapeHtml(String(props[key]))}">${escapeHtml(displayValue)}</span>
        </div>
      `;
    }
    
    dynamicProps.innerHTML = propsHtml || '<p style="color: var(--text-muted); font-size: 11px;">No properties</p>';
  }
}

// Escape HTML for safe display
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Handle edge selection in graph
function onGraphEdgeSelect(edgeData) {
  const detailsPanel = document.getElementById('node-details');
  if (!detailsPanel) return;
  
  detailsPanel.innerHTML = `
    <div class="node-name">Relationship</div>
    <div class="node-type">${edgeData.type}</div>
    <div class="detail-row">
      <span class="detail-label">From</span>
      <span class="detail-value">${edgeData.source}</span>
    </div>
    <div class="detail-row">
      <span class="detail-label">To</span>
      <span class="detail-value">${edgeData.target}</span>
    </div>
  `;
  
  // Hide edit section for edges (or show edge edit options)
  const editSection = document.getElementById('edit-section');
  if (editSection) editSection.style.display = 'none';
}

// Update node details panel
function updateNodeDetailsPanel(nodeData) {
  const detailsPanel = document.getElementById('node-details');
  if (!detailsPanel) return;
  
  let html = `
    <div class="node-name">${nodeData.fullLabel || nodeData.label}</div>
    <div class="node-type">${nodeData.type} (${nodeData.group})</div>
  `;
  
  // Add properties
  const props = nodeData.properties || {};
  for (const [key, value] of Object.entries(props)) {
    if (key === 'name' || value === null || value === undefined) continue;
    
    let displayValue = value;
    if (typeof value === 'object') {
      displayValue = JSON.stringify(value).substring(0, 50) + '...';
    } else if (typeof value === 'string' && value.length > 50) {
      displayValue = value.substring(0, 50) + '...';
    }
    
    html += `
      <div class="detail-row">
        <span class="detail-label">${key.replace(/_/g, ' ')}</span>
        <span class="detail-value">${displayValue}</span>
      </div>
    `;
  }
  
  detailsPanel.innerHTML = html;
}

// Update pending changes display
function updatePendingChangesDisplay() {
  if (!graphRenderer) return;
  
  const counts = graphRenderer.getPendingChangesCount();
  const summaryEl = document.getElementById('pending-changes-summary');
  const listEl = document.getElementById('pending-changes-list');
  const applyBtn = document.getElementById('btn-apply-changes');
  const discardBtn = document.getElementById('btn-discard-changes');
  
  if (counts.total === 0) {
    summaryEl.innerHTML = '<p class="no-changes">No pending changes</p>';
    listEl.innerHTML = '';
    applyBtn.disabled = true;
    discardBtn.disabled = true;
    return;
  }
  
  applyBtn.disabled = false;
  discardBtn.disabled = false;
  
  let summaryHtml = '<div class="change-count">';
  if (counts.nodesCreate + counts.edgesCreate > 0) {
    summaryHtml += `<span class="count-item add">+${counts.nodesCreate + counts.edgesCreate} add</span>`;
  }
  if (counts.nodesUpdate > 0) {
    summaryHtml += `<span class="count-item update">~${counts.nodesUpdate} update</span>`;
  }
  if (counts.nodesDelete + counts.edgesDelete > 0) {
    summaryHtml += `<span class="count-item delete">-${counts.nodesDelete + counts.edgesDelete} delete</span>`;
  }
  summaryHtml += '</div>';
  
  summaryEl.innerHTML = summaryHtml;
  
  // Show pending items in list
  const changes = graphRenderer.getPendingChanges();
  let listHtml = '';
  
  for (const node of changes.nodes.create) {
    listHtml += `<div class="pending-change-item add">${node.type}: ${node.name}</div>`;
  }
  for (const node of changes.nodes.delete) {
    listHtml += `<div class="pending-change-item delete">${node.type}: ${node.name}</div>`;
  }
  for (const edge of changes.edges.create) {
    listHtml += `<div class="pending-change-item add">${edge.sourceName} → ${edge.targetName}</div>`;
  }
  for (const edge of changes.edges.delete) {
    listHtml += `<div class="pending-change-item delete">${edge.sourceName} → ${edge.targetName}</div>`;
  }
  
  listEl.innerHTML = listHtml;
}

// Graph toolbar handlers
document.getElementById('graph-search')?.addEventListener('input', (e) => {
  if (graphRenderer) {
    graphRenderer.search(e.target.value);
  }
});

const GRAPH_FILTER_LABELS = {
  plc: ['AOI', 'Tag', 'PLCTagTable', 'PLCTag'],
  scada: ['UDT', 'Equipment', 'View', 'ViewComponent', 'ScadaTag', 'Script', 'NamedQuery', 'Project', 'GatewayEvent'],
  siemens: ['TiaProject', 'PLCDevice', 'HMIDevice', 'HMIConnection'],
  'siemens-hmi': ['HMIAlarm', 'HMIAlarmClass', 'HMIScript', 'HMIScreen', 'HMITagTable', 'HMITextList'],
  mes: ['Material', 'Batch', 'ProductionOrder', 'Operation', 'CriticalControlPoint', 'ProcessDeviation'],
  troubleshooting: ['FaultSymptom', 'FaultCause', 'OperatorPhrase', 'CommonPhrase', 'Intent'],
  anomaly: ['AgentRun', 'AnomalyEvent'],
  flows: ['DataFlow', 'EndToEndFlow'],
  process: ['ProcessMedium', 'UnitOperation', 'OperatingEnvelope', 'PhysicalPrinciple', 'ChemicalSpecies', 'Reaction'],
};

document.getElementById('graph-filter')?.addEventListener('change', async (e) => {
  const value = e.target.value;
  if (value === 'all') {
    await loadGraphData();
  } else if (GRAPH_FILTER_LABELS[value]) {
    await loadGraphDataFiltered(GRAPH_FILTER_LABELS[value]);
  } else {
    if (graphRenderer) graphRenderer.filterByType(value);
  }
});

async function loadGraphDataFiltered(nodeTypes) {
  const loading = document.getElementById('graph-loading');
  if (loading) loading.classList.add('active');

  try {
    const result = await window.api.graphLoad({ types: nodeTypes });
    if (result.success && graphRenderer) {
      graphRenderer.loadData(result);
      if (loading) loading.classList.remove('active');
    } else {
      console.error('Failed to load filtered graph:', result.error);
      if (loading) loading.innerHTML = `<p>Error: ${result.error}</p>`;
    }
  } catch (error) {
    console.error('Failed to load filtered graph:', error);
    if (loading) loading.innerHTML = `<p>Error: ${error.message}</p>`;
  }
}

document.getElementById('btn-layout-force')?.addEventListener('click', () => {
  if (graphRenderer) {
    graphRenderer.switchLayout('force');
    document.getElementById('btn-layout-force').classList.add('active');
    document.getElementById('btn-layout-hierarchical').classList.remove('active');
  }
});

document.getElementById('btn-layout-hierarchical')?.addEventListener('click', () => {
  if (graphRenderer) {
    graphRenderer.switchLayout('hierarchical');
    document.getElementById('btn-layout-hierarchical').classList.add('active');
    document.getElementById('btn-layout-force').classList.remove('active');
  }
});

document.getElementById('btn-zoom-in')?.addEventListener('click', () => {
  if (graphRenderer) graphRenderer.zoomIn();
});

document.getElementById('btn-zoom-out')?.addEventListener('click', () => {
  if (graphRenderer) graphRenderer.zoomOut();
});

document.getElementById('btn-fit')?.addEventListener('click', () => {
  if (graphRenderer) graphRenderer.fit();
});

document.getElementById('btn-refresh-graph')?.addEventListener('click', () => {
  loadGraphData();
});

// AI drawer toggle
document.getElementById('btn-toggle-ai-drawer')?.addEventListener('click', () => {
  const drawer = document.getElementById('ai-drawer');
  if (drawer) drawer.classList.toggle('expanded');
});

// AI propose button
document.getElementById('btn-ai-propose')?.addEventListener('click', async () => {
  const input = document.getElementById('ai-relationship-input');
  const proposalsEl = document.getElementById('ai-proposals');
  
  if (!input || !input.value.trim()) return;
  
  const description = input.value.trim();
  input.disabled = true;
  proposalsEl.innerHTML = '<p style="color: var(--text-muted);">Analyzing...</p>';
  
  try {
    const result = await window.api.graphAiPropose(description);
    
    if (result.success && result.proposed_changes) {
      let html = '';
      for (const change of result.proposed_changes) {
        html += `
          <div class="ai-proposal">
            <div class="ai-proposal-header">
              <span class="ai-proposal-type">${change.action}</span>
              <span class="ai-proposal-confidence">${Math.round((change.confidence || 0.8) * 100)}% confidence</span>
            </div>
            <div class="ai-proposal-description">
              ${change.source || change.name} ${change.type ? `→ ${change.target}` : ''}
            </div>
            <div class="ai-proposal-actions">
              <button class="btn btn-sm btn-primary" onclick="acceptAiProposal(${JSON.stringify(change).replace(/"/g, '&quot;')})">Accept</button>
              <button class="btn btn-sm btn-ghost" onclick="this.closest('.ai-proposal').remove()">Reject</button>
            </div>
          </div>
        `;
      }
      
      if (result.explanation) {
        html = `<p style="color: var(--text-secondary); margin-bottom: 12px;">${result.explanation}</p>` + html;
      }
      
      proposalsEl.innerHTML = html || '<p>No changes proposed</p>';
    } else {
      proposalsEl.innerHTML = `<p style="color: var(--accent-red);">Error: ${result.error || 'Unknown error'}</p>`;
    }
  } catch (error) {
    proposalsEl.innerHTML = `<p style="color: var(--accent-red);">Error: ${error.message}</p>`;
  }
  
  input.disabled = false;
});

// Accept AI proposal
function acceptAiProposal(change) {
  if (!graphRenderer) return;
  
  if (change.action === 'create_edge') {
    graphRenderer.addPendingEdge({
      sourceType: change.source_type || 'Unknown',
      sourceName: change.source,
      targetType: change.target_type || 'Unknown',
      targetName: change.target,
      type: change.type
    });
  } else if (change.action === 'create_node') {
    graphRenderer.addPendingNode({
      type: change.node_type,
      name: change.name,
      properties: change.properties || {}
    });
  }
  
  updatePendingChangesDisplay();
}

// Make function available globally
window.acceptAiProposal = acceptAiProposal;

// Add node button
document.getElementById('btn-add-node')?.addEventListener('click', () => {
  const typeSelect = document.getElementById('new-node-type');
  const nameInput = document.getElementById('new-node-name');
  
  if (!typeSelect || !nameInput) return;
  
  const nodeType = typeSelect.value;
  const nodeName = nameInput.value.trim();
  
  if (!nodeType || !nodeName) {
    alert('Please select a type and enter a name');
    return;
  }
  
  if (graphRenderer) {
    graphRenderer.addPendingNode({
      type: nodeType,
      name: nodeName,
      properties: {}
    });
    updatePendingChangesDisplay();
    
    // Clear inputs
    nameInput.value = '';
  }
});

// Save node properties
document.getElementById('btn-save-node')?.addEventListener('click', async () => {
  if (!graphState.currentNode) return;
  
  const nodeType = graphState.currentNode.type;
  const nodeName = graphState.currentNode.fullLabel || graphState.currentNode.label;
  
  // Collect property values from EDITABLE inputs only
  const propsContainer = document.getElementById('edit-dynamic-props');
  const properties = {};
  
  if (propsContainer) {
    propsContainer.querySelectorAll('.prop-row.editable').forEach(row => {
      const key = row.dataset.key;
      const input = row.querySelector('.prop-value');
      const value = input?.value;
      if (key && value !== undefined) {
        properties[key] = value;
      }
    });
  }
  
  if (Object.keys(properties).length === 0) {
    alert('No editable properties to save.');
    return;
  }
  
  // Call API to update node
  const btn = document.getElementById('btn-save-node');
  btn.disabled = true;
  btn.textContent = 'Saving...';
  
  try {
    // Capitalize first letter of type for Neo4j label
    const labelType = nodeType.charAt(0).toUpperCase() + nodeType.slice(1);
    const result = await window.api.graphUpdateNode(labelType, nodeName, properties);
    
    if (result.success) {
      alert('Node properties saved!');
      // Reload graph to show updates
      loadGraphData();
    } else {
      alert(`Failed to save: ${result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
  
  btn.disabled = false;
  btn.textContent = 'Save';
});

// Add new property (added as editable)
document.getElementById('btn-add-prop')?.addEventListener('click', () => {
  const keyInput = document.getElementById('new-prop-key');
  const valueInput = document.getElementById('new-prop-value');
  const propsContainer = document.getElementById('edit-dynamic-props');
  
  if (!keyInput || !valueInput || !propsContainer) return;
  
  const key = keyInput.value.trim().toLowerCase().replace(/\s+/g, '_');
  const value = valueInput.value.trim();
  
  if (!key) {
    alert('Please enter a property name');
    return;
  }
  
  // Check if property already exists
  if (propsContainer.querySelector(`[data-key="${key}"]`)) {
    alert(`Property "${key}" already exists`);
    return;
  }
  
  // Find the separator or insert at end of editable section
  const separator = propsContainer.querySelector('.prop-separator');
  
  // Add new property row (as editable)
  const row = document.createElement('div');
  row.className = 'prop-row editable';
  row.dataset.key = key;
  row.innerHTML = `
    <span class="prop-key" title="${key}">${key}</span>
    <textarea class="prop-value" rows="2">${escapeHtml(value)}</textarea>
  `;
  
  if (separator) {
    propsContainer.insertBefore(row, separator);
  } else {
    propsContainer.appendChild(row);
  }
  
  // Clear inputs
  keyInput.value = '';
  valueInput.value = '';
});

// Delete selected node
document.getElementById('btn-delete-selected')?.addEventListener('click', () => {
  if (!graphRenderer || !graphState.currentNode) return;
  
  const confirmed = confirm(`Delete ${graphState.currentNode.type} "${graphState.currentNode.fullLabel || graphState.currentNode.label}"?`);
  if (!confirmed) return;
  
  graphRenderer.markNodeForDeletion(graphState.currentNode.id);
  updatePendingChangesDisplay();
  
  // Clear selection
  graphState.currentNode = null;
  document.getElementById('node-details').innerHTML = '<p class="no-selection">Select a node or edge to view details</p>';
  document.getElementById('edit-section').style.display = 'none';
});

// Add relationship between nodes
document.getElementById('btn-add-relationship')?.addEventListener('click', () => {
  if (!graphRenderer) return;
  
  const source = graphState.relSource;
  const target = graphState.relTarget;
  const relType = document.getElementById('rel-type')?.value;
  
  if (!source || !target) {
    alert('Please select both source and target nodes.\n\nClick a node to set as source, then Shift+click another node to set as target.');
    return;
  }
  
  if (!relType) {
    alert('Please select a relationship type.');
    return;
  }
  
  if (source.id === target.id) {
    alert('Source and target cannot be the same node.');
    return;
  }
  
  // Add to pending changes
  graphRenderer.addPendingEdge({
    sourceId: source.id,
    sourceName: source.name,
    sourceType: source.type,
    targetId: target.id,
    targetName: target.name,
    targetType: target.type,
    type: relType
  });
  
  updatePendingChangesDisplay();
  
  // Clear the form
  clearRelationshipForm();
});

// Clear relationship form
document.getElementById('btn-clear-relationship')?.addEventListener('click', () => {
  clearRelationshipForm();
});

function clearRelationshipForm() {
  graphState.relSource = null;
  graphState.relTarget = null;
  document.getElementById('rel-source').value = '';
  document.getElementById('rel-target').value = '';
  document.getElementById('rel-type').value = '';
}

// Apply pending changes
document.getElementById('btn-apply-changes')?.addEventListener('click', async () => {
  if (!graphRenderer) return;
  
  const changes = graphRenderer.getPendingChanges();
  const btn = document.getElementById('btn-apply-changes');
  
  btn.disabled = true;
  btn.textContent = 'Applying...';
  
  try {
    const result = await window.api.graphApplyBatch(changes);
    
    if (result.success) {
      graphRenderer.commitPendingChanges();
      updatePendingChangesDisplay();
      
      // Reload graph to get updated data
      await loadGraphData();
      
      alert('Changes applied successfully!');
    } else {
      alert(`Failed to apply changes: ${result.errors?.join(', ') || result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
  
  btn.disabled = false;
  btn.textContent = 'Apply All';
});

// Discard pending changes
document.getElementById('btn-discard-changes')?.addEventListener('click', () => {
  if (!graphRenderer) return;
  
  const confirmed = confirm('Discard all pending changes?');
  if (!confirmed) return;
  
  graphRenderer.clearPendingChanges();
  updatePendingChangesDisplay();
});

// ============================================
// Graph Modal (for Browse/Troubleshoot)
// ============================================

function openGraphModal(nodeName, nodeType, title) {
  const modal = document.getElementById('graph-modal');
  const modalTitle = document.getElementById('graph-modal-title');
  const canvas = document.getElementById('graph-modal-canvas');
  
  if (!modal || !canvas) return;
  
  // Set title
  if (modalTitle) modalTitle.textContent = `Graph: ${nodeName}`;
  
  // Show modal first (so canvas has dimensions)
  modal.classList.add('active');
  
  // Store current node for "show more" functionality
  graphState.modalNode = { name: nodeName, type: nodeType };
  
  // Small delay to ensure modal is rendered before creating Cytoscape
  setTimeout(() => {
    // Destroy old renderer if exists (to reinitialize with correct size)
    if (modalGraphRenderer) {
      modalGraphRenderer.destroy();
      modalGraphRenderer = null;
    }
    
    // Create modal renderer
    modalGraphRenderer = new GraphRenderer(canvas, {
      editable: false,
      layout: 'force',  // Force layout works better for small subgraphs
      onNodeSelect: onModalNodeSelect
    });
    
    // Set up double-click for expansion
    modalGraphRenderer.onNodeDoubleClick = (nodeData) => {
      loadModalNeighbors(nodeData.fullLabel || nodeData.label, nodeData.type);
    };
    
    // Load neighbors
    loadModalNeighbors(nodeName, nodeType);
  }, 100);
}

async function loadModalNeighbors(nodeName, nodeType, hops = 1) {
  const detailsEl = document.getElementById('modal-node-details');
  
  try {
    console.log(`[Graph Modal] Loading neighbors for ${nodeType}:${nodeName} with ${hops} hops`);
    
    // First try direct neighbor lookup
    let result = await window.api.graphNeighbors({
      nodeId: nodeName,
      nodeType: nodeType,
      hops: hops,
      maxNodes: 30
    });
    
    // If that fails, try without type constraint
    if (!result.success || !result.nodes || result.nodes.length === 0) {
      console.log('[Graph Modal] Direct lookup failed, trying without type...');
      result = await window.api.graphNeighbors({
        nodeId: nodeName,
        hops: hops,
        maxNodes: 30
      });
    }
    
    // If still no results, try searching
    if (!result.success || !result.nodes || result.nodes.length === 0) {
      console.log('[Graph Modal] Neighbor lookup failed, trying search...');
      const searchResult = await window.api.graphSearch(nodeName, { limit: 10 });
      
      if (searchResult.success && searchResult.nodes && searchResult.nodes.length > 0) {
        // Found nodes via search - use the first match
        const firstNode = searchResult.nodes[0];
        console.log('[Graph Modal] Found via search:', firstNode.label);
        
        result = await window.api.graphNeighbors({
          nodeId: firstNode.label,
          nodeType: firstNode.type,
          hops: hops,
          maxNodes: 30
        });
      }
    }
    
    console.log('[Graph Modal] Final result:', result);
    
    if (result.success && modalGraphRenderer) {
      if (result.nodes && result.nodes.length > 0) {
        modalGraphRenderer.loadData(result);
        // Fit view after loading
        setTimeout(() => modalGraphRenderer.fit(), 200);
      } else {
        showNodeNotFoundMessage(nodeName, detailsEl);
      }
    } else if (!result.success) {
      console.error('[Graph Modal] Error:', result.error);
      showNodeNotFoundMessage(nodeName, detailsEl);
    }
  } catch (error) {
    console.error('Failed to load modal neighbors:', error);
    if (detailsEl) {
      detailsEl.innerHTML = `<p class="no-selection">Error loading graph: ${error.message}</p>`;
    }
  }
}

// Show a helpful message when node is not found in the database
function showNodeNotFoundMessage(nodeName, detailsEl) {
  if (!detailsEl) return;
  
  detailsEl.innerHTML = `
    <div class="node-not-found">
      <p><strong>"${nodeName}"</strong> was not found in the ontology database.</p>
      <p class="hint">This item exists in the source data but hasn't been ingested into Neo4j yet.</p>
      <p class="hint">Use the <strong>Ingest</strong> tab to load source files, or browse existing nodes in the <strong>Graph</strong> tab.</p>
    </div>
  `;
}

function onModalNodeSelect(nodeData) {
  const detailsEl = document.getElementById('modal-node-details');
  if (!detailsEl) return;
  
  let html = `
    <div class="node-name">${nodeData.fullLabel || nodeData.label}</div>
    <div class="node-type">${nodeData.type}</div>
  `;
  
  const props = nodeData.properties || {};
  for (const [key, value] of Object.entries(props)) {
    if (key === 'name' || value === null || value === undefined) continue;
    
    let displayValue = value;
    if (typeof value === 'string' && value.length > 30) {
      displayValue = value.substring(0, 30) + '...';
    }
    
    html += `
      <div class="detail-row">
        <span class="detail-label">${key.replace(/_/g, ' ')}</span>
        <span class="detail-value">${displayValue}</span>
      </div>
    `;
  }
  
  detailsEl.innerHTML = html;
}

// Close modal
document.getElementById('btn-close-modal')?.addEventListener('click', () => {
  const modal = document.getElementById('graph-modal');
  if (modal) modal.classList.remove('active');
});

// Modal layout buttons
document.getElementById('modal-btn-layout-force')?.addEventListener('click', () => {
  if (modalGraphRenderer) {
    modalGraphRenderer.switchLayout('force');
    document.getElementById('modal-btn-layout-force').classList.add('active');
    document.getElementById('modal-btn-layout-hierarchical').classList.remove('active');
  }
});

document.getElementById('modal-btn-layout-hierarchical')?.addEventListener('click', () => {
  if (modalGraphRenderer) {
    modalGraphRenderer.switchLayout('hierarchical');
    document.getElementById('modal-btn-layout-hierarchical').classList.add('active');
    document.getElementById('modal-btn-layout-force').classList.remove('active');
  }
});

document.getElementById('modal-btn-zoom-in')?.addEventListener('click', () => {
  if (modalGraphRenderer) modalGraphRenderer.zoomIn();
});

document.getElementById('modal-btn-zoom-out')?.addEventListener('click', () => {
  if (modalGraphRenderer) modalGraphRenderer.zoomOut();
});

document.getElementById('modal-btn-fit')?.addEventListener('click', () => {
  if (modalGraphRenderer) modalGraphRenderer.fit();
});

// Show more neighbors
document.getElementById('btn-show-more')?.addEventListener('click', () => {
  if (!graphState.modalNode) return;
  loadModalNeighbors(graphState.modalNode.name, graphState.modalNode.type, 2);
});

// Open in graph tab
document.getElementById('btn-open-in-graph-tab')?.addEventListener('click', () => {
  const modal = document.getElementById('graph-modal');
  if (modal) modal.classList.remove('active');
  
  // Switch to graph tab
  navButtons.forEach(b => b.classList.remove('active'));
  document.querySelector('[data-tab="graph"]')?.classList.add('active');
  
  tabContents.forEach(tab => tab.classList.remove('active'));
  document.getElementById('tab-graph')?.classList.add('active');
  
  // Initialize graph tab if needed
  initGraphTab();
  
  // Load the node's neighbors
  if (graphState.modalNode) {
    loadNodeNeighbors(graphState.modalNode.name, graphState.modalNode.type);
  }
});

// Close modal on background click
document.getElementById('graph-modal')?.addEventListener('click', (e) => {
  if (e.target.id === 'graph-modal') {
    e.target.classList.remove('active');
  }
});

// Escape key closes modal
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    const modal = document.getElementById('graph-modal');
    if (modal && modal.classList.contains('active')) {
      modal.classList.remove('active');
    }
  }
});

// Make openGraphModal available globally
window.openGraphModal = openGraphModal;

// ============================================
// DEXPI P&ID Tab
// ============================================

let dexpiGraphRenderer = null;
let dexpiState = {
  loaded: false,
  data: null,
};

// DEXPI-specific Cytoscape styling (overrides for P&ID look)
function createDexpiGraphRenderer(container) {
  const renderer = new GraphRenderer(container, {
    editable: false,
    layout: 'hierarchical',
    onNodeSelect: onDexpiNodeSelect,
    onEdgeSelect: onDexpiEdgeSelect,
  });

  // Override styles for DEXPI-specific P&ID look
  if (renderer.cy) {
    renderer.cy.style()
      .selector('node')
      .style({
        'label': 'data(label)',
        'text-valign': 'bottom',
        'text-halign': 'center',
        'text-margin-y': 8,
        'font-size': 10,
        'font-family': 'Inter, sans-serif',
        'color': '#b0b0c0',
        'text-outline-width': 2,
        'text-outline-color': '#0a0a0f',
        'background-color': 'data(color)',
        'shape': 'data(shape)',
        'width': 38,
        'height': 38,
        'border-width': 2,
        'border-color': '#2a2a3a',
        'transition-property': 'width, height, border-width, border-color',
        'transition-duration': '0.15s',
      })
      .selector('node:selected')
      .style({
        'border-width': 3,
        'border-color': '#00d4ff',
        'width': 46,
        'height': 46,
        'color': '#ffffff',
        'font-size': 11,
        'font-weight': 600,
      })
      .selector('node.hovered')
      .style({
        'width': 46,
        'height': 46,
        'border-width': 3,
        'border-color': '#00d4ff',
        'color': '#ffffff',
        'font-size': 12,
        'font-weight': 600,
        'z-index': 999,
      })
      .selector('node.neighbor-highlighted')
      .style({
        'border-width': 2,
        'border-color': '#5a5a6a',
        'color': '#c0c0d0',
        'font-size': 11,
      })
      .selector('node.faded')
      .style({
        'opacity': 0.2,
        'label': '',
      })
      .selector('edge')
      .style({
        'width': 1.5,
        'line-color': 'data(edgeColor)',
        'target-arrow-color': 'data(edgeColor)',
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier',
        'line-style': 'data(lineStyle)',
        'opacity': 0.5,
        'label': '',
      })
      .selector('edge.highlighted')
      .style({
        'width': 2.5,
        'label': 'data(label)',
        'font-size': 9,
        'color': '#a0a0b0',
        'text-rotation': 'autorotate',
        'text-margin-y': -10,
        'text-outline-width': 2,
        'text-outline-color': '#0a0a0f',
        'opacity': 1,
      })
      .selector('edge.faded')
      .style({
        'opacity': 0.08,
      })
      .selector('edge:selected')
      .style({
        'width': 3,
        'line-color': '#00d4ff',
        'target-arrow-color': '#00d4ff',
        'label': 'data(label)',
        'font-size': 9,
        'color': '#00d4ff',
        'text-rotation': 'autorotate',
        'text-margin-y': -10,
        'text-outline-width': 2,
        'text-outline-color': '#0a0a0f',
        'opacity': 1,
      })
      .selector('.dimmed')
      .style({
        'opacity': 0.15,
      })
      .update();
  }

  return renderer;
}

// Load DEXPI data into the graph renderer
function loadDexpiData(data) {
  if (!dexpiGraphRenderer) return;

  const elements = [];

  // Add nodes with DEXPI-specific data fields
  if (data.nodes) {
    for (const node of data.nodes) {
      elements.push({
        data: {
          id: node.id,
          label: truncateLabel(node.label, 25),
          fullLabel: node.label,
          type: node.type,
          dexpiCategory: node.dexpiCategory,
          dexpiLabel: node.dexpiLabel,
          color: node.color || '#616161',
          shape: node.shape || 'round-rectangle',
          icon: node.icon || '?',
          group: 'dexpi',
          properties: node.properties || {},
        },
      });
    }
  }

  // Add edges with DEXPI-specific styling
  if (data.edges) {
    for (const edge of data.edges) {
      elements.push({
        data: {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          type: edge.type,
          dexpiType: edge.dexpiType,
          label: edge.label || edge.type,
          edgeColor: edge.color || '#9E9E9E',
          lineStyle: edge.lineStyle || 'solid',
          properties: edge.properties || {},
        },
      });
    }
  }

  dexpiGraphRenderer.cy.elements().remove();
  dexpiGraphRenderer.cy.add(elements);
  dexpiGraphRenderer.runLayout();
}

function truncateLabel(label, maxLen) {
  if (!label) return '';
  if (label.length <= maxLen) return label;
  return label.substring(0, maxLen - 3) + '...';
}

// Initialize DEXPI tab
function initDexpiTab() {
  if (dexpiState.loaded) return;

  const container = document.getElementById('dexpi-graph-container');
  if (!container) return;

  // Check pydexpi availability
  window.api.dexpiCheck().then(result => {
    const statusEl = document.getElementById('dexpi-pydexpi-status');
    if (statusEl) {
      if (result.pydexpiAvailable) {
        statusEl.textContent = `pydexpi v${result.pydexpiVersion} available`;
        statusEl.style.color = 'var(--color-success)';
      } else {
        statusEl.textContent = 'pydexpi not installed (export limited)';
        statusEl.style.color = 'var(--color-warning)';
      }
    }
  });

  dexpiState.loaded = true;
}

// Convert and load DEXPI graph
async function convertToDexpi() {
  const loading = document.getElementById('dexpi-graph-loading');
  if (loading) {
    loading.innerHTML = '<div class="loading-spinner"></div><p>Converting to DEXPI P&ID view...</p>';
    loading.classList.add('active');
  }

  const limit = parseInt(document.getElementById('dexpi-limit')?.value || '500', 10);
  const includeScada = document.getElementById('dexpi-include-scada')?.checked || false;
  const includeTroubleshooting = document.getElementById('dexpi-include-troubleshooting')?.checked || false;

  try {
    const result = await window.api.dexpiConvert({
      limit,
      includeScada,
      includeTroubleshooting,
    });

    if (result.success) {
      dexpiState.data = result;

      // Create renderer if needed
      const container = document.getElementById('dexpi-graph-container');
      if (dexpiGraphRenderer) {
        dexpiGraphRenderer.destroy();
        dexpiGraphRenderer = null;
      }
      dexpiGraphRenderer = createDexpiGraphRenderer(container);

      // Load data
      loadDexpiData(result);

      if (loading) loading.classList.remove('active');

      // Update stats and legend
      updateDexpiStats(result);
      updateDexpiLegend(result);
    } else {
      if (loading) {
        loading.innerHTML = `<p style="color: var(--color-danger);">Error: ${result.error}</p>`;
      }
    }
  } catch (error) {
    if (loading) {
      loading.innerHTML = `<p style="color: var(--color-danger);">Error: ${error.message}</p>`;
    }
  }
}

// Update DEXPI statistics panel
function updateDexpiStats(result) {
  const statsEl = document.getElementById('dexpi-stats');
  if (!statsEl) return;

  const stats = result.stats || {};
  let html = `
    <div class="dexpi-stats-summary">
      <div class="stat-row"><span>Total Nodes</span><span class="stat-value">${result.nodeCount || 0}</span></div>
      <div class="stat-row"><span>Total Edges</span><span class="stat-value">${result.edgeCount || 0}</span></div>
    </div>
    <div class="dexpi-stats-breakdown">
  `;

  const categoryColors = {
    equipment: '#1565C0',
    instrument: '#2E7D32',
    actuator: '#E65100',
    piping: '#546E7A',
    safety: '#C62828',
    process_control: '#00838F',
    nozzle: '#6A1B9A',
    scada_hmi: '#7B1FA2',
    data_interface: '#F57F17',
    unclassified: '#616161',
  };

  const categoryLabels = {
    equipment: 'Equipment',
    instrument: 'Instrumentation',
    actuator: 'Actuators',
    piping: 'Piping',
    safety: 'Safety',
    process_control: 'Process Control',
    nozzle: 'Nozzles',
    scada_hmi: 'SCADA / HMI',
    data_interface: 'Data Interface',
    unclassified: 'Unclassified',
  };

  for (const [cat, count] of Object.entries(stats)) {
    if (count === 0) continue;
    const color = categoryColors[cat] || '#616161';
    const label = categoryLabels[cat] || cat;
    html += `
      <div class="stat-row">
        <span><span class="legend-color-inline" style="background: ${color};"></span>${label}</span>
        <span class="stat-value">${count}</span>
      </div>
    `;
  }

  html += '</div>';
  statsEl.innerHTML = html;
}

// Dynamically populate DEXPI legend from conversion result
function updateDexpiLegend(result) {
  const legendBody = document.getElementById('dexpi-legend-body');
  if (!legendBody) return;

  const legend = result.legend || [];
  if (legend.length === 0) {
    legendBody.innerHTML = '<p class="text-muted text-xs">No data yet</p>';
    return;
  }

  // Separate node and edge legend entries
  const nodeEntries = legend.filter(e => e.type === 'node');
  const edgeEntries = legend.filter(e => e.type === 'edge');

  // Group node entries by logical sections
  const shapeToClass = {
    'ellipse': 'shape-ellipse',
    'diamond': 'shape-diamond',
    'round-rectangle': 'shape-round-rectangle',
  };

  let html = '';

  // Node categories (only show those with count > 0 or all if none have counts)
  const hasAnyCounts = nodeEntries.some(e => e.count > 0);
  const visibleNodes = hasAnyCounts ? nodeEntries.filter(e => e.count > 0) : nodeEntries;

  if (visibleNodes.length > 0) {
    html += '<div class="legend-group"><div class="legend-group-title">Node Types</div>';
    for (const entry of visibleNodes) {
      const shapeClass = shapeToClass[entry.shape] || 'shape-round-rectangle';
      const countStr = entry.count > 0 ? ` (${entry.count})` : '';
      html += `
        <div class="legend-item">
          <span class="legend-shape ${shapeClass}" style="background: ${entry.color};"></span>
          <span class="legend-label">${entry.label}${countStr}</span>
        </div>`;
    }
    html += '</div>';
  }

  // Edge types
  if (edgeEntries.length > 0) {
    html += '<div class="legend-group"><div class="legend-group-title">Connections</div>';
    for (const entry of edgeEntries) {
      const lineStyle = entry.style || 'solid';
      html += `
        <div class="legend-item">
          <span class="legend-line" style="border-top: 2px ${lineStyle} ${entry.color};"></span>
          <span class="legend-label">${entry.label}</span>
        </div>`;
    }
    html += '</div>';
  }

  legendBody.innerHTML = html;
}

// Handle DEXPI node selection
function onDexpiNodeSelect(nodeData) {
  const detailsEl = document.getElementById('dexpi-node-details');
  if (!detailsEl) return;

  const fullLabel = nodeData.fullLabel || nodeData.label;
  const dexpiLabel = nodeData.dexpiLabel || 'Unknown';
  const dexpiCategory = nodeData.dexpiCategory || 'unclassified';
  const color = nodeData.color || '#616161';

  let html = `
    <div class="dexpi-item-header">
      <span class="dexpi-item-badge" style="background: ${color};">${nodeData.icon || '?'}</span>
      <div>
        <div class="node-name">${fullLabel}</div>
        <div class="node-type" style="color: ${color};">${dexpiLabel}</div>
      </div>
    </div>
    <div class="detail-row">
      <span class="detail-label">DEXPI Category</span>
      <span class="detail-value">${dexpiCategory}</span>
    </div>
    <div class="detail-row">
      <span class="detail-label">Internal Type</span>
      <span class="detail-value">${nodeData.type}</span>
    </div>
  `;

  const props = nodeData.properties || {};
  const importantProps = ['purpose', 'description', 'inferred_purpose', 'vendor', 'revision'];

  for (const key of importantProps) {
    if (props[key]) {
      let displayValue = props[key];
      if (typeof displayValue === 'string' && displayValue.length > 80) {
        displayValue = displayValue.substring(0, 80) + '...';
      }
      html += `
        <div class="detail-row">
          <span class="detail-label">${key.replace(/_/g, ' ')}</span>
          <span class="detail-value">${displayValue}</span>
        </div>
      `;
    }
  }

  // Show remaining properties
  for (const [key, value] of Object.entries(props)) {
    if (importantProps.includes(key) || key === 'name' || value === null || value === undefined) continue;
    if (typeof value === 'object') continue;

    let displayValue = String(value);
    if (displayValue.length > 50) {
      displayValue = displayValue.substring(0, 50) + '...';
    }

    html += `
      <div class="detail-row">
        <span class="detail-label">${key.replace(/_/g, ' ')}</span>
        <span class="detail-value">${displayValue}</span>
      </div>
    `;
  }

  detailsEl.innerHTML = html;
}

// Handle DEXPI edge selection
function onDexpiEdgeSelect(edgeData) {
  const detailsEl = document.getElementById('dexpi-node-details');
  if (!detailsEl) return;

  const dexpiType = edgeData.dexpiType || 'unknown';
  const edgeColor = edgeData.edgeColor || '#9E9E9E';

  detailsEl.innerHTML = `
    <div class="node-name">Connection</div>
    <div class="node-type" style="color: ${edgeColor};">${edgeData.label || edgeData.type}</div>
    <div class="detail-row">
      <span class="detail-label">DEXPI Type</span>
      <span class="detail-value">${dexpiType}</span>
    </div>
    <div class="detail-row">
      <span class="detail-label">Internal Type</span>
      <span class="detail-value">${edgeData.type}</span>
    </div>
    <div class="detail-row">
      <span class="detail-label">From</span>
      <span class="detail-value">${edgeData.source}</span>
    </div>
    <div class="detail-row">
      <span class="detail-label">To</span>
      <span class="detail-value">${edgeData.target}</span>
    </div>
  `;
}

// DEXPI toolbar handlers
document.getElementById('btn-dexpi-convert')?.addEventListener('click', convertToDexpi);

document.getElementById('btn-dexpi-export')?.addEventListener('click', async () => {
  try {
    const result = await window.api.dexpiExport();
    if (result.success && result.outputFile) {
      alert(`DEXPI JSON exported!\n\nFile: ${result.outputFile}`);
    } else if (result.error !== 'Export cancelled') {
      alert(`Error: ${result.error}`);
    }
  } catch (error) {
    alert(`Error: ${error.message}`);
  }
});

document.getElementById('dexpi-search')?.addEventListener('input', (e) => {
  if (dexpiGraphRenderer) {
    dexpiGraphRenderer.search(e.target.value);
  }
});

document.getElementById('dexpi-filter')?.addEventListener('change', (e) => {
  if (!dexpiGraphRenderer) return;

  const filterValue = e.target.value;
  if (filterValue === 'all') {
    dexpiGraphRenderer.cy.elements().removeClass('dimmed');
  } else {
    dexpiGraphRenderer.cy.nodes().forEach(node => {
      if (node.data('dexpiCategory') === filterValue) {
        node.removeClass('dimmed');
        node.connectedEdges().removeClass('dimmed');
      } else {
        node.addClass('dimmed');
      }
    });
  }
});

document.getElementById('btn-dexpi-layout-force')?.addEventListener('click', () => {
  if (dexpiGraphRenderer) {
    dexpiGraphRenderer.switchLayout('force');
    document.getElementById('btn-dexpi-layout-force').classList.add('active');
    document.getElementById('btn-dexpi-layout-hierarchical').classList.remove('active');
  }
});

document.getElementById('btn-dexpi-layout-hierarchical')?.addEventListener('click', () => {
  if (dexpiGraphRenderer) {
    dexpiGraphRenderer.switchLayout('hierarchical');
    document.getElementById('btn-dexpi-layout-hierarchical').classList.add('active');
    document.getElementById('btn-dexpi-layout-force').classList.remove('active');
  }
});

document.getElementById('btn-dexpi-zoom-in')?.addEventListener('click', () => {
  if (dexpiGraphRenderer) dexpiGraphRenderer.zoomIn();
});

document.getElementById('btn-dexpi-zoom-out')?.addEventListener('click', () => {
  if (dexpiGraphRenderer) dexpiGraphRenderer.zoomOut();
});

document.getElementById('btn-dexpi-fit')?.addEventListener('click', () => {
  if (dexpiGraphRenderer) dexpiGraphRenderer.fit();
});

// ============================================
// Initial Load
// ============================================

// Initialize Browse tab
initBrowseSubTabs();

// ============================================
// Settings Tab
// ============================================

const settingsUrlInput = document.getElementById('settings-ignition-url');
const settingsTokenInput = document.getElementById('settings-ignition-token');
const btnToggleToken = document.getElementById('btn-toggle-token');
const btnTestIgnition = document.getElementById('btn-test-ignition');
const btnSaveSettings = document.getElementById('btn-save-settings');
const connectionStatusEl = document.getElementById('ignition-connection-status');
const ignitionSidebarStatus = document.getElementById('ignition-api-status');

// Toggle password visibility
btnToggleToken?.addEventListener('click', () => {
  const isPassword = settingsTokenInput.type === 'password';
  settingsTokenInput.type = isPassword ? 'text' : 'password';
  btnToggleToken.querySelector('.icon-eye').classList.toggle('hidden', !isPassword);
  btnToggleToken.querySelector('.icon-eye-off').classList.toggle('hidden', isPassword);
});

async function loadSettings() {
  const result = await api.getSettings();
  if (result.success) {
    settingsUrlInput.value = result.ignitionApiUrl || '';
    settingsTokenInput.value = result.ignitionApiToken || '';
    updateIgnitionSidebarStatus(!!result.ignitionApiUrl);

    if (result.ignitionApiUrl) {
      autoTestIgnitionConnection(result.ignitionApiUrl, result.ignitionApiToken || '');
    }
  }
}

async function autoTestIgnitionConnection(url, token) {
  try {
    const result = await api.testIgnitionConnection({ url, token });
    updateIgnitionSidebarStatus(true, !!result.success);
  } catch {
    updateIgnitionSidebarStatus(true, false);
  }
}

function updateConnectionStatus(state, text) {
  const dot = connectionStatusEl.querySelector('.status-dot');
  const label = connectionStatusEl.querySelector('.connection-status-text');

  dot.className = 'status-dot';
  if (state === 'connected') dot.classList.add('connected');
  else if (state === 'error') dot.classList.add('error');

  label.textContent = text;
}

function updateIgnitionSidebarStatus(configured, connected) {
  if (!ignitionSidebarStatus) return;
  const dot = ignitionSidebarStatus.querySelector('.status-dot');
  const label = ignitionSidebarStatus.querySelector('.status-text');

  dot.className = 'status-dot';
  if (connected) {
    dot.classList.add('connected');
    label.textContent = 'Ignition Connected';
  } else if (configured) {
    label.textContent = 'Ignition API';
  } else {
    label.textContent = 'Ignition API';
  }
}

btnTestIgnition?.addEventListener('click', async () => {
  const url = settingsUrlInput.value.trim();
  const token = settingsTokenInput.value.trim();

  if (!url) {
    updateConnectionStatus('error', 'Enter a gateway URL first');
    return;
  }

  updateConnectionStatus('', 'Testing...');
  btnTestIgnition.disabled = true;

  try {
    const result = await api.testIgnitionConnection({ url, token });
    if (result.success) {
      const parts = [];
      if (result.version) parts.push(`v${result.version}`);
      if (result.state) parts.push(result.state);
      if (result.platform) parts.push(result.platform);
      const detail = parts.length > 0 ? ` (${parts.join(', ')})` : '';
      updateConnectionStatus('connected', `Connected${detail}`);
      updateIgnitionSidebarStatus(true, true);
    } else {
      updateConnectionStatus('error', result.error || 'Connection failed');
      updateIgnitionSidebarStatus(true, false);
    }
  } catch (err) {
    updateConnectionStatus('error', err.message || 'Connection failed');
    updateIgnitionSidebarStatus(true, false);
  } finally {
    btnTestIgnition.disabled = false;
  }
});

btnSaveSettings?.addEventListener('click', async () => {
  const url = settingsUrlInput.value.trim();
  const token = settingsTokenInput.value.trim();

  btnSaveSettings.disabled = true;
  btnSaveSettings.textContent = 'Saving...';

  try {
    const result = await api.saveSettings({
      ignitionApiUrl: url,
      ignitionApiToken: token,
    });

    if (result.success) {
      btnSaveSettings.textContent = 'Saved';
      updateIgnitionSidebarStatus(!!url);
      if (url) {
        autoTestIgnitionConnection(url, token);
      }
      setTimeout(() => { btnSaveSettings.textContent = 'Save Settings'; }, 1500);
    } else {
      btnSaveSettings.textContent = 'Error';
      setTimeout(() => { btnSaveSettings.textContent = 'Save Settings'; }, 2000);
    }
  } catch (err) {
    btnSaveSettings.textContent = 'Error';
    setTimeout(() => { btnSaveSettings.textContent = 'Save Settings'; }, 2000);
  } finally {
    btnSaveSettings.disabled = false;
  }
});

// ============================================
// Artifact Ingestion (P&IDs / SOPs / Diagrams)
// ============================================

const btnSelectArtifact = document.getElementById('btn-select-artifact');
const btnIngestArtifact = document.getElementById('btn-ingest-artifact');
const artifactSourceKind = document.getElementById('artifact-source-kind');
const artifactFileList = document.getElementById('artifact-file-list');
const artifactIngestStatus = document.getElementById('artifact-ingest-status');

let selectedArtifactFiles = [];

btnSelectArtifact?.addEventListener('click', async () => {
  const extensions = ['png', 'jpg', 'jpeg', 'bmp', 'tiff', 'tif', 'webp', 'gif', 'pdf', 'txt', 'md'];
  const result = await api.selectFile({
    filters: [{ name: 'Supported Files', extensions }],
    multiple: true,
  });

  if (result && result.filePaths && result.filePaths.length > 0) {
    selectedArtifactFiles = result.filePaths;
    if (artifactFileList) {
      artifactFileList.innerHTML = selectedArtifactFiles
        .map(f => `<div>${f.split(/[\\/]/).pop()}</div>`)
        .join('');
    }
    if (btnIngestArtifact) btnIngestArtifact.disabled = false;
  }
});

btnIngestArtifact?.addEventListener('click', async () => {
  if (selectedArtifactFiles.length === 0) return;

  const sourceKind = artifactSourceKind ? artifactSourceKind.value : 'pid';
  btnIngestArtifact.disabled = true;
  btnIngestArtifact.textContent = 'Ingesting...';

  appendOutput(`\n[Artifact Ingest] Processing ${selectedArtifactFiles.length} file(s) as ${sourceKind}...\n`);

  try {
    const files = selectedArtifactFiles.map(p => ({ path: p, sourceKind }));
    const result = await api.ingestArtifactBatch(files);

    if (result.success) {
      if (result.node_details && result.node_details.length > 0) {
        appendOutput(`[Artifact Ingest] Node updates (${result.node_details.length}):\n`);
        for (const d of result.node_details) {
          appendOutput(`  + ${d}\n`);
        }
      }
      if (result.concept_details && result.concept_details.length > 0) {
        appendOutput(`[Artifact Ingest] Process concepts (${result.concept_details.length}):\n`);
        for (const d of result.concept_details) {
          appendOutput(`  + ${d}\n`);
        }
      }
      if (result.relationship_details && result.relationship_details.length > 0) {
        appendOutput(`[Artifact Ingest] Relationships (${result.relationship_details.length}):\n`);
        for (const d of result.relationship_details) {
          appendOutput(`  ~ ${d}\n`);
        }
      }
      appendOutput(
        `[Artifact Ingest] Summary: ${result.nodes_updated || 0} node updates, ` +
        `${result.concepts_created || 0} process concepts, ` +
        `${result.relationships_created || 0} relationships\n`
      );
      if (result.errors && result.errors.length > 0) {
        appendOutput(`[Artifact Ingest] ${result.errors.length} error(s):\n`);
        for (const err of result.errors) {
          appendOutput(`  - ${typeof err === 'string' ? err : JSON.stringify(err)}\n`);
        }
      }
      if (artifactIngestStatus) {
        artifactIngestStatus.style.display = 'block';
        artifactIngestStatus.textContent =
          `${result.nodes_updated || 0} updates, ${result.concepts_created || 0} concepts, ${result.relationships_created || 0} rels`;
      }
    } else {
      appendOutput(`[Artifact Ingest] Error: ${result.error || 'Unknown error'}\n`);
    }
  } catch (err) {
    appendOutput(`[Artifact Ingest] Error: ${err.message}\n`);
  } finally {
    btnIngestArtifact.disabled = false;
    btnIngestArtifact.textContent = 'Ingest';
  }
});

// ============================================
// Database Connections Settings
// ============================================

const dbConnectionsList = document.getElementById('db-connections-list');
const dbConnectionsActions = document.getElementById('db-connections-actions');
const btnSaveDbCreds = document.getElementById('btn-save-db-creds');
const dbSidebarStatus = document.getElementById('db-connections-status');

async function loadDbConnections() {
  if (!dbConnectionsList) return;
  try {
    const result = await api.getDbConnections();
    if (!result.success || !result.connections || result.connections.length === 0) {
      dbConnectionsList.innerHTML =
        '<p class="text-muted text-sm">No database connections found. Ingest a project to discover them.</p>';
      dbConnectionsActions.style.display = 'none';
      updateDbSidebarStatus(false);
      return;
    }

    const conns = result.connections;
    let html = '';
    for (const conn of conns) {
      const typeBadge = conn.database_type || 'Unknown';
      const statusClass = conn.hasPassword ? 'connected' : '';
      const statusText = conn.hasPassword ? 'Credentials set' : 'No password';
      html += `
        <div class="db-conn-row" data-conn-name="${conn.name}" style="margin-bottom: var(--space-3); padding: var(--space-3); border: 1px solid var(--border); border-radius: var(--radius-md);">
          <div style="display: flex; align-items: center; gap: var(--space-2); margin-bottom: var(--space-2);">
            <strong>${conn.name}</strong>
            <span class="badge" style="font-size: 0.7rem; padding: 2px 6px; background: var(--surface-2); border-radius: var(--radius-sm);">${typeBadge}</span>
            <span class="text-muted text-xs" style="margin-left: auto;">${conn.url}</span>
          </div>
          <div style="display: grid; grid-template-columns: 1fr 1fr auto; gap: var(--space-2); align-items: end;">
            <div class="form-group" style="margin: 0;">
              <label class="text-xs">Username</label>
              <input type="text" class="input db-username" value="${conn.savedUsername || conn.username || ''}" placeholder="username" autocomplete="off" style="font-size: 0.85rem;">
            </div>
            <div class="form-group" style="margin: 0;">
              <label class="text-xs">Password</label>
              <input type="password" class="input db-password" value="" placeholder="${conn.hasPassword ? '••••••••' : 'not set'}" autocomplete="off" style="font-size: 0.85rem;">
            </div>
            <button class="btn btn-sm btn-secondary btn-test-db" data-conn="${conn.name}" style="height: 36px;" title="Test connection">Test</button>
          </div>
          <div class="connection-status db-conn-status" style="margin-top: var(--space-1);">
            <span class="status-dot ${statusClass}"></span>
            <span class="connection-status-text text-xs">${statusText}</span>
          </div>
        </div>
      `;
    }
    dbConnectionsList.innerHTML = html;
    dbConnectionsActions.style.display = '';
    updateDbSidebarStatus(true, conns.some(c => c.hasPassword));

    // Attach test button handlers
    dbConnectionsList.querySelectorAll('.btn-test-db').forEach(btn => {
      btn.addEventListener('click', async () => {
        const connName = btn.dataset.conn;
        const row = btn.closest('.db-conn-row');
        const statusDot = row.querySelector('.status-dot');
        const statusText = row.querySelector('.connection-status-text');

        // Save credentials first before testing
        const username = row.querySelector('.db-username').value.trim();
        const password = row.querySelector('.db-password').value;
        if (username || password) {
          const creds = {};
          creds[connName] = { username, password };
          await api.saveDbCredentials(creds);
        }

        btn.disabled = true;
        btn.textContent = '...';
        statusDot.className = 'status-dot';
        statusText.textContent = 'Testing...';

        try {
          const result = await api.testDbConnection(connName);
          if (result.success) {
            statusDot.className = 'status-dot connected';
            statusText.textContent = 'Connected';
          } else {
            statusDot.className = 'status-dot error';
            statusText.textContent = result.error || 'Failed';
          }
        } catch (err) {
          statusDot.className = 'status-dot error';
          statusText.textContent = err.message || 'Failed';
        } finally {
          btn.disabled = false;
          btn.textContent = 'Test';
        }
      });
    });
  } catch (err) {
    dbConnectionsList.innerHTML =
      `<p class="text-muted text-sm">Error loading connections: ${err.message}</p>`;
  }
}

function updateDbSidebarStatus(hasConnections, anyConfigured) {
  if (!dbSidebarStatus) return;
  const dot = dbSidebarStatus.querySelector('.status-dot');
  const label = dbSidebarStatus.querySelector('.status-text');

  dot.className = 'status-dot';
  if (anyConfigured) {
    dot.classList.add('connected');
    label.textContent = 'DB Connected';
  } else if (hasConnections) {
    label.textContent = 'DB Connections';
  } else {
    label.textContent = 'DB Connections';
  }
}

btnSaveDbCreds?.addEventListener('click', async () => {
  const rows = dbConnectionsList.querySelectorAll('.db-conn-row');
  const credentials = {};
  let count = 0;

  rows.forEach(row => {
    const connName = row.dataset.connName;
    const username = row.querySelector('.db-username').value.trim();
    const password = row.querySelector('.db-password').value;
    if (username || password) {
      credentials[connName] = { username, password };
      count++;
    }
  });

  if (count === 0) return;

  btnSaveDbCreds.disabled = true;
  btnSaveDbCreds.textContent = 'Saving...';

  try {
    const result = await api.saveDbCredentials(credentials);
    if (result.success) {
      btnSaveDbCreds.textContent = 'Saved';
      setTimeout(() => { btnSaveDbCreds.textContent = 'Save Credentials'; }, 1500);
    } else {
      btnSaveDbCreds.textContent = 'Error';
      setTimeout(() => { btnSaveDbCreds.textContent = 'Save Credentials'; }, 2000);
    }
  } catch {
    btnSaveDbCreds.textContent = 'Error';
    setTimeout(() => { btnSaveDbCreds.textContent = 'Save Credentials'; }, 2000);
  } finally {
    btnSaveDbCreds.disabled = false;
  }
});
// Agents Tab — Per-subsystem agent monitoring
// ============================================

const HEALTH_TREND_MAX_CYCLES = 20;

const agentsState = {
  runId: null,
  status: 'idle',
  events: [],
  selectedEventId: null,
  selectedSubsystemId: null,
  listenersReady: false,
  subsystemHealth: {},
  subsystemOrder: [],
  subsystemHistory: {},
  agentStates: {},
  pendingDeepAnalyze: new Set(),
  eventActionStatus: {},
};

function setAgentEventActionStatus(eventId, action, message, options = {}) {
  if (!eventId) return;
  agentsState.eventActionStatus[eventId] = {
    action,
    message: String(message || ''),
    tone: options.tone || 'pending',
    buttonLabel: options.buttonLabel || '',
  };
  renderSubsystemHealthGrid();
}

function clearAgentEventActionStatus(eventId) {
  if (!eventId || !agentsState.eventActionStatus[eventId]) return;
  delete agentsState.eventActionStatus[eventId];
  renderSubsystemHealthGrid();
}

function getAgentsElements() {
  return {
    btnStart: document.getElementById('btn-agents-start'),
    btnStop: document.getElementById('btn-agents-stop'),
    btnRefresh: document.getElementById('btn-agents-refresh'),
    btnCleanup: document.getElementById('btn-agents-cleanup'),
    statusChip: document.getElementById('agents-status-chip'),
    statusText: document.getElementById('agents-status-text'),
    filterState: document.getElementById('agents-filter-state'),
    filterSeverity: document.getElementById('agents-filter-severity'),
    filterSearch: document.getElementById('agents-filter-search'),
    cfgPoll: document.getElementById('agents-config-poll-ms'),
    cfgHist: document.getElementById('agents-config-history-min'),
    cfgPoints: document.getElementById('agents-config-min-points'),
    cfgAutoLlm: document.getElementById('agents-config-auto-llm'),
    cfgMaxLlm: document.getElementById('agents-config-max-llm'),
    cfgZ: document.getElementById('agents-config-threshold-z'),
    cfgMad: document.getElementById('agents-config-threshold-mad'),
    cfgStale: document.getElementById('agents-config-staleness-sec'),
  };
}

function getAgentsConfigFromUI() {
  const el = getAgentsElements();
  return {
    pollIntervalMs: Number(el.cfgPoll?.value || 1000),
    historyWindowMinutes: Number(el.cfgHist?.value || 360),
    minHistoryPoints: Number(el.cfgPoints?.value || 30),
    maxCandidatesPerSubsystem: 8,
    maxLlmTriagesPerCycle: el.cfgAutoLlm?.checked ? Number(el.cfgMaxLlm?.value || 5) : 0,
    maxLlmTriagesPerSubsystem: el.cfgAutoLlm?.checked ? 2 : 0,
    thresholds: {
      z: Number(el.cfgZ?.value || 3),
      mad: Number(el.cfgMad?.value || 3.5),
      stalenessSec: Number(el.cfgStale?.value || 120),
    },
    scope: {
      subsystemMode: 'auto',
      subsystemPriority: ['view', 'equipment', 'group', 'global'],
      includeUnlinkedTags: false,
    },
  };
}

function formatAgentTime(ts) {
  if (!ts) return 'n/a';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return String(ts);
  return d.toLocaleTimeString();
}

function computeHealthLevel(signal) {
  const maxAbsZ = parseFloat(signal.maxAbsZ || 0);
  if (maxAbsZ >= 5) return 'critical';
  if (maxAbsZ >= 3) return 'warning';
  if (maxAbsZ >= 1.5) return 'elevated';
  return 'healthy';
}

function healthLevelToScore(level) {
  return { healthy: 0.1, elevated: 0.4, warning: 0.7, critical: 1.0 }[level] || 0.1;
}

function getSubsystemIdForEvent(event) {
  return event.subsystem_id
    || `${(event.subsystem_type || 'global')}:${(event.subsystem_name || 'all').toLowerCase()}`;
}

function getFilteredEventsForSubsystem(subId) {
  const el = getAgentsElements();
  const stateFilter = (el.filterState?.value || '').toLowerCase();
  const sevFilter = (el.filterSeverity?.value || '').toLowerCase();
  const search = (el.filterSearch?.value || '').trim().toLowerCase();
  return agentsState.events.filter((event) => {
    if (getSubsystemIdForEvent(event) !== subId) return false;
    if (stateFilter && String(event.state || '').toLowerCase() !== stateFilter) return false;
    if (sevFilter && String(event.severity || '').toLowerCase() !== sevFilter) return false;
    if (search) {
      const haystack = [event.summary, event.source_tag, event.tag_name]
        .filter(Boolean).join(' ').toLowerCase();
      if (!haystack.includes(search)) return false;
    }
    return true;
  });
}

function ensureSubsystemOrder(subId) {
  if (!subId) return;
  if (!agentsState.subsystemOrder.includes(subId)) {
    agentsState.subsystemOrder.push(subId);
  }
}

function getPreferredEventIdForSubsystem(subId) {
  const events = getFilteredEventsForSubsystem(subId);
  if (!events.length) return null;
  if (agentsState.selectedEventId && events.some((event) => event.event_id === agentsState.selectedEventId)) {
    return agentsState.selectedEventId;
  }
  return events[0].event_id;
}

function updateSubsystemHealthFromStatus(payload) {
  const diagnostics = payload.diagnostics || {};
  const phase = diagnostics.phase || '';
  const subId = payload.subsystemId || diagnostics.subsystemId;

  if (diagnostics.subsystemTagMap && typeof diagnostics.subsystemTagMap === 'object') {
    for (const [sid, info] of Object.entries(diagnostics.subsystemTagMap)) {
      if (!agentsState.subsystemHealth[sid]) {
        agentsState.subsystemHealth[sid] = {
          subsystemId: sid,
          subsystemType: info.type || 'global',
          subsystemName: info.name || sid,
          evaluated: (info.tags || []).length,
          candidate: 0, nearShift: 0, maxAbsZ: 0, avgAbsZ: 0,
          healthLevel: 'healthy',
          tagSignals: (info.tags || []).map((t) => ({
            path: t.path, name: t.name || t.path, z: 0, mad: 0, value: null,
          })),
        };
        agentsState.agentStates[sid] = {
          state: 'running', cycleCount: 0, avgCycleMs: 0, totalCandidates: 0, totalTriaged: 0,
        };
        ensureSubsystemOrder(sid);
      }
    }
  }

  if (subId && phase === 'cycle_complete') {
    const signals = diagnostics.subsystemShiftSignals;
    if (Array.isArray(signals) && signals.length) {
      for (const sig of signals) {
        const sid = sig.subsystemId || subId;
        const healthLevel = computeHealthLevel(sig);
        agentsState.subsystemHealth[sid] = { ...sig, healthLevel };
        ensureSubsystemOrder(sid);
        if (!agentsState.subsystemHistory[sid]) agentsState.subsystemHistory[sid] = [];
        const history = agentsState.subsystemHistory[sid];
        history.push({
          healthLevel,
          avgAbsZ: parseFloat(sig.avgAbsZ || 0),
          candidateRatio: parseFloat(sig.candidateRatio || 0),
          candidates: parseInt(sig.candidate || 0, 10),
          evaluated: parseInt(sig.evaluated || 0, 10),
          ts: Date.now(),
        });
        if (history.length > HEALTH_TREND_MAX_CYCLES) history.splice(0, history.length - HEALTH_TREND_MAX_CYCLES);
      }
    }
    agentsState.agentStates[subId] = {
      state: payload.state || 'running',
      cycleCount: diagnostics.cycleCount || 0,
      avgCycleMs: diagnostics.avgCycleMs || 0,
      totalCandidates: diagnostics.totalCandidates || 0,
      totalTriaged: diagnostics.totalTriaged || 0,
    };

    // Replace events for this subsystem with current live events
    const liveEvents = payload.liveEvents || [];
    agentsState.events = agentsState.events.filter((e) => e.subsystem_id !== subId);
    for (const evt of liveEvents) {
      agentsState.events.unshift(evt);
    }
  }

  if (subId && phase === 'cycle_progress') {
    if (!agentsState.agentStates[subId]) {
      agentsState.agentStates[subId] = { state: 'running', cycleCount: 0, avgCycleMs: 0, totalCandidates: 0, totalTriaged: 0 };
    }
    const step = diagnostics.step || '';
    const stepLabels = {
      reading_tags: 'Reading tags',
      fetching_history: 'Fetching history',
      scoring: 'Scoring',
      triaging: 'Triaging',
      waiting: 'Idle',
    };
    agentsState.agentStates[subId].currentStep = step;
    agentsState.agentStates[subId].stepLabel = stepLabels[step] || step;
    agentsState.agentStates[subId].stepDetail = diagnostics.detail || '';
    agentsState.agentStates[subId].lastStepAt = Date.now();
    updateAgentCardPhase(subId);
    return;
  }

  if (subId && (phase === 'agent_paused' || phase === 'agent_stopped')) {
    if (agentsState.agentStates[subId]) {
      agentsState.agentStates[subId].state = 'paused';
      agentsState.agentStates[subId].currentStep = 'paused';
      agentsState.agentStates[subId].stepLabel = 'Paused';
    }
  }
  if (subId && (phase === 'agent_resumed' || phase === 'agent_started')) {
    if (agentsState.agentStates[subId]) {
      agentsState.agentStates[subId].state = 'running';
      agentsState.agentStates[subId].currentStep = '';
      agentsState.agentStates[subId].stepLabel = '';
    }
  }

  renderSubsystemHealthGrid();
}

function updateAgentCardPhase(subId) {
  const card = document.querySelector(`.agents-health-card[data-subsystem-id="${CSS.escape(subId)}"]`);
  if (!card) return;
  const phaseEl = card.querySelector('.health-agent-phase');
  if (!phaseEl) return;
  const agState = agentsState.agentStates[subId] || {};
  const step = agState.currentStep || '';
  const isActive = step && step !== 'waiting' && step !== 'paused';
  phaseEl.textContent = agState.stepLabel || '';
  phaseEl.className = 'health-agent-phase' + (isActive ? ' phase-active' : '');
  if (isActive) {
    card.classList.add('agent-cycling');
  } else {
    card.classList.remove('agent-cycling');
  }
}

function renderSubsystemHealthGrid() {
  const container = document.getElementById('agents-health-grid');
  if (!container) return;

  const entries = Object.entries(agentsState.subsystemHealth);
  if (!entries.length) {
    container.innerHTML = '<div class="agents-health-empty">Start monitoring to see subsystem agents.</div>';
    return;
  }

  const orderMap = new Map(agentsState.subsystemOrder.map((subId, index) => [subId, index]));
  entries.sort((a, b) => {
    const ia = orderMap.get(a[0]) ?? Number.MAX_SAFE_INTEGER;
    const ib = orderMap.get(b[0]) ?? Number.MAX_SAFE_INTEGER;
    return ia - ib;
  });

  container.innerHTML = entries
    .map(([subId, sig]) => {
      const level = sig.healthLevel || 'healthy';
      const isExpanded = agentsState.selectedSubsystemId === subId;
      const expandedClass = isExpanded ? ' expanded selected' : '';
      const name = sig.subsystemName || subId;
      const type = sig.subsystemType || 'global';
      const evaluated = parseInt(sig.evaluated || 0, 10);
      const candidates = parseInt(sig.candidate || 0, 10);
      const maxZ = parseFloat(sig.maxAbsZ || 0).toFixed(1);
      const anomalyClass = candidates > 0 ? (level === 'critical' ? ' has-critical' : ' has-anomalies') : '';
      const history = agentsState.subsystemHistory[subId] || [];

      const agState = agentsState.agentStates[subId] || {};
      const isPaused = agState.state === 'paused';
      const agentIcon = isPaused ? '&#9654;' : '&#9646;&#9646;';
      const agentTitle = isPaused ? 'Resume agent' : 'Pause agent';

      const currentStep = agState.currentStep || '';
      const isActive = currentStep && currentStep !== 'waiting' && currentStep !== 'paused';
      const phaseLabel = agState.stepLabel || '';
      const cyclingClass = isActive ? ' agent-cycling' : '';

      const metricsRow = `
        <div class="health-agent-metrics">
          <span class="health-agent-phase${isActive ? ' phase-active' : ''}">${escapeHtml(phaseLabel)}</span>
          <span title="Cycles">#${agState.cycleCount || 0}</span>
          <span title="Avg cycle time">${agState.avgCycleMs || 0}ms</span>
          <span title="Total candidates">cand: ${agState.totalCandidates || 0}</span>
          <span title="Total triaged">tri: ${agState.totalTriaged || 0}</span>
        </div>
      `;

      let expandedBody = '';
      if (isExpanded) {
        const bigTrend = renderTrendBars(history, 48);
        const tagRows = renderTagSignalRows(sig.tagSignals || []);
        const tagCount = (sig.tagSignals || []).length;
        const subEvents = getFilteredEventsForSubsystem(subId);
        const preferredEventId = getPreferredEventIdForSubsystem(subId);
        if (preferredEventId && preferredEventId !== agentsState.selectedEventId) {
          agentsState.selectedEventId = preferredEventId;
        }
        const eventRows = renderSubsystemEventRows(subEvents);
        const eventCount = subEvents.length;
        expandedBody = `
          <div class="health-expanded-body">
            <div class="health-expanded-trend">${bigTrend}</div>
            <div class="health-tag-list-header">
              <h4>Events</h4>
              <span>${eventCount} live event${eventCount === 1 ? '' : 's'}</span>
            </div>
            <div class="health-event-list">${eventRows}</div>
            <div class="health-tag-list-header" style="margin-top:var(--space-3)">
              <h4>Tags</h4>
              <span>${tagCount} tags</span>
            </div>
            <div class="health-tag-col-headers">
              <span>Name</span><span>Trend</span><span>z-score</span><span>Avg</span><span>Current</span>
            </div>
            <div class="health-tag-list">${tagRows}</div>
          </div>
        `;
      } else {
        expandedBody = `<div class="health-trend">${renderTrendBars(history, 28)}</div>`;
      }

      return `
        <div class="agents-health-card health-${escapeHtml(level)}${expandedClass}${isPaused ? ' agent-paused' : ''}${cyclingClass}" data-subsystem-id="${escapeHtml(subId)}">
          <div class="health-card-top">
            <div class="health-card-identity">
              <span class="health-indicator health-${escapeHtml(level)}"></span>
              <span class="health-card-name" title="${escapeHtml(name)}">${escapeHtml(name)}</span>
            </div>
            <div class="health-card-controls">
              <button class="btn-agent-toggle" data-subsystem-id="${escapeHtml(subId)}" title="${agentTitle}">${agentIcon}</button>
              <span class="health-card-type">${escapeHtml(type)}</span>
            </div>
          </div>
          ${metricsRow}
          <div class="health-card-stats">
            <div class="health-stat">
              <span class="health-stat-label">Tags</span>
              <span class="health-stat-value">${evaluated}</span>
            </div>
            <div class="health-stat">
              <span class="health-stat-label">Candidates</span>
              <span class="health-stat-value${anomalyClass}">${candidates}</span>
            </div>
            <div class="health-stat">
              <span class="health-stat-label">Peak z</span>
              <span class="health-stat-value">${maxZ}</span>
            </div>
          </div>
          ${expandedBody}
          <span class="health-card-health-label health-${escapeHtml(level)}">${escapeHtml(level)}</span>
        </div>
      `;
    })
    .join('');

  container.querySelectorAll('.btn-agent-toggle').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const subId = btn.getAttribute('data-subsystem-id');
      if (!subId) return;
      const agState = agentsState.agentStates[subId] || {};
      if (agState.state === 'paused') window.api.agentsStartSubsystem(subId);
      else window.api.agentsStopSubsystem(subId);
    });
  });

  container.querySelectorAll('.agents-health-card').forEach((card) => {
    card.addEventListener('click', (e) => {
      if (e.target.closest('.health-tag-list') || e.target.closest('.health-event-list') || e.target.closest('.btn-agent-toggle')) return;
      const subId = card.getAttribute('data-subsystem-id');
      selectSubsystem(subId);
    });
  });

  container.querySelectorAll('.health-event-row').forEach((row) => {
    row.addEventListener('click', (e) => {
      e.stopPropagation();
      const eventId = row.getAttribute('data-event-id');
      if (eventId) selectAgentEvent(eventId);
    });
  });

  container.querySelectorAll('.health-event-detail-actions .btn-ack-event').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const eventId = btn.getAttribute('data-event-id');
      if (eventId) acknowledgeEvent(eventId);
    });
  });

  container.querySelectorAll('.health-event-detail-actions .btn-create-case').forEach((btn) => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const eventId = btn.getAttribute('data-event-id');
      if (eventId) await createCaseFromAgentEvent(eventId, btn);
    });
  });

  container.querySelectorAll('.health-event-detail-actions .btn-open-graph').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const eventId = btn.getAttribute('data-event-id');
      if (!eventId) return;
      const event = agentsState.events.find((ev) => ev.event_id === eventId);
      if (!event) return;
      const target = resolveAgentGraphTarget(event);
      if (target) openGraphModal(target.name, target.type, event.summary || target.name);
    });
  });
}

function renderSubsystemEventRows(events) {
  if (!events.length) return '<div class="health-tag-empty">No events for this subsystem.</div>';
  return events.slice(0, 50).map((event) => {
    const sev = String(event.severity || 'low').toLowerCase();
    const isSelected = event.event_id === agentsState.selectedEventId;
    const selectedClass = isSelected ? ' selected' : '';
    const tagLabel = event.tag_name || event.source_tag || '';
    const timeLabel = formatAgentTime(event.created_at);

    let detailHtml = '';
    if (isSelected) {
      detailHtml = renderInlineEventDetail(event);
    }

    return `
      <div class="health-event-row${selectedClass}" data-event-id="${escapeHtml(event.event_id || '')}">
        <div class="health-event-row-header">
          <span class="agents-severity sev-${escapeHtml(sev)}">${escapeHtml(sev)}</span>
          <span class="health-event-summary">${escapeHtml(event.summary || 'Anomaly')}</span>
          <span class="health-event-tag">${escapeHtml(tagLabel)}</span>
          <span class="health-event-time">${escapeHtml(timeLabel)}</span>
        </div>
        ${detailHtml}
      </div>
    `;
  }).join('');
}

function renderInlineEventDetail(event) {
  let checks = [], causes = [], safety = [];
  try { checks = JSON.parse(event.recommended_checks_json || '[]'); } catch (e) {}
  try { causes = JSON.parse(event.probable_causes_json || '[]'); } catch (e) {}
  try { safety = JSON.parse(event.safety_notes_json || '[]'); } catch (e) {}

  const st = String(event.state || '').toLowerCase();
  const ackLabel = st === 'acknowledged' ? 'Clear' : (st === 'cleared' ? 'Cleared' : 'Acknowledge');
  const ackDisabled = st === 'cleared' ? ' disabled' : '';
  const actionState = agentsState.eventActionStatus[event.event_id] || null;
  const actionPending = actionState?.tone === 'pending';
  const createLabel = actionState?.action === 'create-case'
    ? (actionState.buttonLabel || 'Creating...')
    : 'Create Case';
  const createDisabled = actionPending ? ' disabled' : '';
  const actionStatusHtml = actionState?.message
    ? `<div class="health-event-action-status tone-${escapeHtml(actionState.tone || 'pending')}">${escapeHtml(actionState.message)}</div>`
    : '';

  return `
    <div class="health-event-detail" onclick="event.stopPropagation()">
      <div class="health-event-detail-grid">
        <span class="detail-label">State</span><span>${escapeHtml(event.state || 'open')}</span>
        <span class="detail-label">z-score</span><span>${escapeHtml(String(event.z_score ?? '0'))}</span>
        <span class="detail-label">MAD</span><span>${escapeHtml(String(event.mad_score ?? '0'))}</span>
        <span class="detail-label">Confidence</span><span>${escapeHtml(String(event.confidence ?? ''))}</span>
        <span class="detail-label">Category</span><span>${escapeHtml(event.category || '')}</span>
        <span class="detail-label">Source Tag</span><span>${escapeHtml(event.source_tag || '')}</span>
      </div>
      ${event.explanation ? `<div class="detail-section"><span class="detail-label">Explanation</span><div>${escapeHtml(event.explanation)}</div></div>` : ''}
      ${causes.length ? `<div class="detail-section"><span class="detail-label">Probable Causes</span><ul class="agents-list">${causes.map((x) => `<li>${escapeHtml(String(x))}</li>`).join('')}</ul></div>` : ''}
      ${checks.length ? `<div class="detail-section"><span class="detail-label">Checks</span><ul class="agents-list">${checks.map((x) => `<li>${escapeHtml(String(x))}</li>`).join('')}</ul></div>` : ''}
      ${safety.length ? `<div class="detail-section"><span class="detail-label">Safety</span><ul class="agents-list">${safety.map((x) => `<li>${escapeHtml(String(x))}</li>`).join('')}</ul></div>` : ''}
      <div class="health-event-detail-actions">
        <button class="btn btn-sm btn-primary btn-create-case" data-event-id="${escapeHtml(event.event_id)}"${createDisabled}>${createLabel}</button>
        <button class="btn btn-sm btn-secondary btn-open-graph" data-event-id="${escapeHtml(event.event_id)}">Open in Graph</button>
        <button class="btn btn-sm btn-ghost btn-ack-event" data-event-id="${escapeHtml(event.event_id)}"${ackDisabled}>${ackLabel}</button>
      </div>
      ${actionStatusHtml}
    </div>
  `;
}

function renderTrendBars(history, maxHeight) {
  const h = maxHeight || 28;
  const slots = HEALTH_TREND_MAX_CYCLES;
  const bars = [];
  for (let i = 0; i < slots; i++) {
    const idx = history.length - slots + i;
    if (idx < 0) {
      bars.push('<div class="health-trend-bar trend-empty" style="height: 3px"></div>');
      continue;
    }
    const entry = history[idx];
    const level = entry.healthLevel || 'healthy';
    const score = healthLevelToScore(level);
    const height = Math.max(3, Math.round(score * h));
    bars.push(`<div class="health-trend-bar trend-${escapeHtml(level)}" style="height: ${height}px"></div>`);
  }
  return bars.join('');
}

function tagZToHealthLevel(absZ) {
  if (absZ >= 5) return 'critical';
  if (absZ >= 2.5) return 'warning';
  if (absZ >= 1.5) return 'elevated';
  return 'healthy';
}

function renderSparklineSvg(values, width, height) {
  if (!values || values.length < 2) {
    return `<svg width="${width}" height="${height}" class="tag-sparkline"><line x1="0" y1="${height / 2}" x2="${width}" y2="${height / 2}" stroke="var(--color-border)" stroke-width="1"/></svg>`;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const pad = 1;
  const usableH = height - pad * 2;
  const step = width / (values.length - 1);
  const points = values
    .map((v, i) => `${(i * step).toFixed(1)},${(pad + usableH - ((v - min) / range) * usableH).toFixed(1)}`)
    .join(' ');
  return `<svg width="${width}" height="${height}" class="tag-sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"><polyline points="${points}" fill="none" stroke="var(--color-accent)" stroke-width="1.5" vector-effect="non-scaling-stroke"/></svg>`;
}

function renderTagSignalRows(tagSignals) {
  if (!tagSignals || !tagSignals.length) {
    return '<div class="health-tag-empty">No tag data available yet.</div>';
  }
  return tagSignals
    .map((tag) => {
      const absZ = Math.abs(tag.z || 0);
      const level = tagZToHealthLevel(absZ);
      const currentVal = tag.value != null ? String(tag.value) : '—';
      const avgVal = tag.avg != null ? String(tag.avg) : '—';
      const zDisplay = (tag.z || 0).toFixed(2);
      const sparkline = tag.sparkline && tag.sparkline.length >= 2
        ? renderSparklineSvg(tag.sparkline, 120, 24)
        : renderSparklineSvg(null, 120, 24);
      return `
        <div class="health-tag-row" title="${escapeHtml(tag.path || tag.name || '')}">
          <span class="health-tag-name">${escapeHtml(tag.name || tag.path || '')}</span>
          <div class="health-tag-sparkline">${sparkline}</div>
          <span class="health-tag-zscore tag-z-${escapeHtml(level)}">z ${escapeHtml(zDisplay)}</span>
          <span class="health-tag-avg" title="Avg over window">${escapeHtml(avgVal)}</span>
          <span class="health-tag-value" title="Current">${escapeHtml(currentVal)}</span>
        </div>
      `;
    })
    .join('');
}

function selectSubsystem(subId) {
  const clearBtn = document.getElementById('btn-agents-clear-subsystem');
  if (agentsState.selectedSubsystemId === subId) {
    agentsState.selectedSubsystemId = null;
    agentsState.selectedEventId = null;
    if (clearBtn) clearBtn.style.display = 'none';
  } else {
    agentsState.selectedSubsystemId = subId;
    agentsState.selectedEventId = getPreferredEventIdForSubsystem(subId);
    if (clearBtn) clearBtn.style.display = '';
  }
  renderSubsystemHealthGrid();
}

function selectAgentEvent(eventId) {
  if (agentsState.selectedEventId === eventId) {
    agentsState.selectedEventId = null;
  } else {
    agentsState.selectedEventId = eventId;
  }
  renderSubsystemHealthGrid();
}

function updateAgentStatusUi(status, text) {
  const el = getAgentsElements();
  if (!el.statusChip || !el.statusText) return;
  el.statusChip.className = 'status-chip';
  const normalized = (status || 'idle').toLowerCase();
  if (normalized === 'running') el.statusChip.classList.add('running');
  if (normalized === 'failed' || normalized === 'error') el.statusChip.classList.add('error');
  el.statusChip.textContent = normalized;
  el.statusText.textContent = text || normalized;
  if (el.btnStart) el.btnStart.disabled = normalized === 'running' || normalized === 'starting';
  if (el.btnStop) el.btnStop.disabled = !(normalized === 'running' || normalized === 'starting' || normalized === 'stopping');
}

function resolveAgentGraphTarget(event) {
  if (String(event.subsystem_type || '').toLowerCase() === 'view' && event.subsystem_name) {
    return { name: event.subsystem_name, type: 'View' };
  }
  const equipment = (event.equipment || []).find(Boolean);
  if (equipment) return { name: equipment, type: 'Equipment' };
  if (String(event.subsystem_type || '').toLowerCase() === 'equipment' && event.subsystem_name) {
    return { name: event.subsystem_name, type: 'Equipment' };
  }
  const tagName = event.tag_name || (event.tags || []).find(Boolean) || event.source_tag;
  if (tagName) return { name: tagName, type: 'ScadaTag' };
  return null;
}

async function loadAgentEvents() {
  const el = getAgentsElements();
  const result = await window.api.agentsListEvents({
    limit: 200,
    state: el.filterState?.value || undefined,
    severity: el.filterSeverity?.value || undefined,
    runId: agentsState.runId || undefined,
  });
  if (!result.success) return;
  agentsState.events = Array.isArray(result.events) ? result.events : [];
  renderSubsystemHealthGrid();
}

async function refreshAgentStatus() {
  const status = await window.api.agentsStatus(agentsState.runId || undefined);
  if (!status.success) {
    updateAgentStatusUi('error', status.error || 'Failed to fetch status');
    return;
  }
  if (status.active) {
    agentsState.runId = status.runId || agentsState.runId;
    agentsState.status = status.status || 'running';
    updateAgentStatusUi(agentsState.status, `Run ${agentsState.runId}`);
  } else {
    agentsState.status = 'idle';
    updateAgentStatusUi('idle', 'No active run');
  }
}

async function startAgentsMonitoring() {
  const config = getAgentsConfigFromUI();
  agentsState.subsystemHealth = {};
  agentsState.subsystemOrder = [];
  agentsState.subsystemHistory = {};
  agentsState.agentStates = {};
  agentsState.selectedSubsystemId = null;
  agentsState.selectedEventId = null;
  agentsState.events = [];
  renderSubsystemHealthGrid();
  const clearSubBtn = document.getElementById('btn-agents-clear-subsystem');
  if (clearSubBtn) clearSubBtn.style.display = 'none';
  const result = await window.api.agentsStart(config);
  if (!result.success) {
    console.error('[Agents start failed]', result);
    updateAgentStatusUi('error', result.error || 'Failed to start monitoring');
    return;
  }
  console.log('[Agents] started, runId=' + (result.runId || 'n/a'));
  agentsState.runId = result.runId;
  agentsState.status = 'running';
  updateAgentStatusUi('running', `Run ${result.runId}`);
  await loadAgentEvents();
}

async function stopAgentsMonitoring() {
  const result = await window.api.agentsStop(agentsState.runId || undefined);
  if (!result.success) {
    updateAgentStatusUi('error', result.error || 'Failed to stop monitoring');
    return;
  }
  agentsState.status = 'stopped';
  updateAgentStatusUi('stopped', 'Monitoring stopped');
}

async function deepAnalyzeEvent(eventId, btnEl) {
  const event = agentsState.events.find((e) => e.event_id === eventId);
  if (!event) {
    console.error('[Agents] deep-analyze: event not found in local state', eventId);
    if (btnEl) { btnEl.textContent = 'Not Found'; btnEl.disabled = false; }
    return;
  }
  agentsState.pendingDeepAnalyze.add(eventId);
  if (btnEl) { btnEl.disabled = true; btnEl.textContent = 'Analyzing…'; }
  try {
    const result = await window.api.agentsDeepAnalyze(eventId, event);
    if (!result.success) {
      console.error('[Agents] deep-analyze failed:', result.error);
      agentsState.pendingDeepAnalyze.delete(eventId);
      if (btnEl) { btnEl.textContent = 'Failed — Retry'; btnEl.disabled = false; }
    }
    // Button stays disabled — result arrives async via AGENT_EVENT with deepAnalyze=true
  } catch (err) {
    console.error('[Agents] deep-analyze error:', err);
    agentsState.pendingDeepAnalyze.delete(eventId);
    if (btnEl) { btnEl.textContent = 'Failed — Retry'; btnEl.disabled = false; }
  }
}

async function acknowledgeEvent(eventId) {
  const event = agentsState.events.find((e) => e.event_id === eventId);
  const st = String(event?.state || '').toLowerCase();
  const result = st === 'acknowledged'
    ? await window.api.agentsClearEvent(eventId, '')
    : await window.api.agentsAckEvent(eventId, '');
  if (!result.success) return;
  const idx = agentsState.events.findIndex((e) => e.event_id === eventId);
  if (idx >= 0) {
    agentsState.events[idx].state = st === 'acknowledged' ? 'cleared' : 'acknowledged';
  }
  renderSubsystemHealthGrid();
}

function upsertRealtimeAgentEvent(payload) {
  const evt = payload?.event;
  if (!evt || !evt.event_id) return;
  if (payload.deepAnalyze) {
    agentsState.pendingDeepAnalyze.delete(evt.event_id);
    if (evt.deep_analyze_error) {
      console.error('[Agents] Deep analyze failed:', evt.deep_analyze_error);
    } else {
      console.log('[Agents] Deep analyze complete for', evt.event_id);
    }
  }
  const idx = agentsState.events.findIndex((e) => e.event_id === evt.event_id);
  if (idx >= 0) agentsState.events[idx] = { ...agentsState.events[idx], ...evt };
  else agentsState.events.unshift(evt);
  renderSubsystemHealthGrid();
}

// ============================================
// Cases Tab — Investigation workspace
// ============================================

const casesState = {
  initialized: false,
  cases: [],
  selectedCaseId: null,
  currentCase: null,
  currentReport: null,
  isLoadingList: false,
  isLoadingDetail: false,
  statusOverride: null,
  actionState: null,
  logStreamIds: new Set(),
  logListenersReady: false,
  assistantSessions: {},
  assistantListenersReady: false,
  listRequestSeq: 0,
  detailRequestSeq: 0,
};

function getCasesElements() {
  return {
    list: document.getElementById('cases-list'),
    detail: document.getElementById('cases-detail'),
    statusChip: document.getElementById('cases-status-chip'),
    statusText: document.getElementById('cases-status-text'),
    countLabel: document.getElementById('cases-count-label'),
    filterStatus: document.getElementById('cases-filter-status'),
    btnRefresh: document.getElementById('btn-cases-refresh'),
    btnGenerate: document.getElementById('btn-cases-generate-report'),
    btnSaveReport: document.getElementById('btn-cases-save-report'),
    log: document.getElementById('cases-log'),
    btnClearLog: document.getElementById('btn-clear-cases-log'),
  };
}

function appendCasesLog(text, clear = false) {
  const log = getCasesElements().log;
  if (!log) return;
  if (clear) log.textContent = '';
  log.textContent += text;
  log.scrollTop = log.scrollHeight;
}

function createCaseStreamContext(actionLabel, target = 'cases') {
  const streamId = `cases-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  casesState.logStreamIds.add(streamId);
  appendCasesLog(`\n[${actionLabel}] ${new Date().toLocaleTimeString()}\n`);
  return {
    streamId,
    target,
  };
}

function completeCaseStream(streamId, success) {
  if (!streamId) return;
  if (!casesState.logStreamIds.has(streamId)) return;
  casesState.logStreamIds.delete(streamId);
  appendCasesLog(success ? '[OK] Case action complete.\n' : '[ERROR] Case action failed.\n');
}

function appendCasesUiDebugLog(message) {
  appendCasesLog(`[UI DEBUG] ${message}\n`);
}

function ensureCasesLogListeners() {
  if (casesState.logListenersReady) return;
  casesState.logListenersReady = true;

  window.api.onStreamOutput((data) => {
    if (!['cases', 'cases-assistant'].includes(data?.target) || !data.streamId || !casesState.logStreamIds.has(data.streamId)) return;
    if (data.type === 'claude-stream') return;
    if (data.type === 'stderr') {
      const text = String(data.text || '');
      if (!text.includes('GqlStatusObject') && !text.includes('Received notification')) {
        appendCasesLog(text.endsWith('\n') ? text : `${text}\n`);
      }
      return;
    }
    if (data.text) appendCasesLog(`${data.text}\n`);
  });

  window.api.onToolCall((data) => {
    if (!['cases', 'cases-assistant'].includes(data?.target) || !data.streamId || !casesState.logStreamIds.has(data.streamId)) return;
    appendCasesLog(`[TOOL] ${data.tool}\n`);
  });

  window.api.onStreamComplete((data) => {
    if (!['cases', 'cases-assistant'].includes(data?.target) || !data.streamId) return;
    completeCaseStream(data.streamId, Boolean(data.success));
  });
}

function setCasesStatusOverride(chip, text, tone = 'pending') {
  casesState.statusOverride = {
    chip: String(chip || 'Loading'),
    text: String(text || ''),
    tone,
  };
  updateCasesToolbar();
}

function clearCasesStatusOverride() {
  if (!casesState.statusOverride) return;
  casesState.statusOverride = null;
  updateCasesToolbar();
}

function setCaseActionState(action, message, options = {}) {
  casesState.actionState = {
    action,
    message: String(message || ''),
    tone: options.tone || 'pending',
    buttonLabel: options.buttonLabel || '',
  };
  if (options.statusChip || options.statusText) {
    setCasesStatusOverride(options.statusChip || 'Working', options.statusText || message || '', options.tone || 'pending');
  }
  updateCasesToolbar();
  renderCaseDetail();
}

function clearCaseActionState() {
  if (!casesState.actionState) return;
  casesState.actionState = null;
  updateCasesToolbar();
  renderCaseDetail();
}

function getCaseAssistantSession(caseId) {
  if (!caseId) return { history: [], turns: [] };
  if (!casesState.assistantSessions[caseId]) {
    casesState.assistantSessions[caseId] = {
      history: [],
      turns: [],
    };
  }
  return casesState.assistantSessions[caseId];
}

function buildCaseAssistantContext(caseData) {
  if (!caseData) return '';
  const event = caseData.event || {};
  const tags = Array.isArray(caseData.tags) ? caseData.tags : [];
  const equipment = Array.isArray(caseData.equipment) ? caseData.equipment : [];
  return [
    `Case ID: ${caseData.case_id || ''}`,
    `Title: ${caseData.title || ''}`,
    `Status: ${caseData.status || ''}`,
    `Severity: ${caseData.severity || ''}`,
    `Subsystem: ${caseData.subsystem_name || caseData.subsystem_id || ''}`,
    `Source tag: ${caseData.source_tag || event.source_tag || ''}`,
    `Event summary: ${event.summary || caseData.summary || ''}`,
    `Operator context: ${caseData.operator_context || ''}`,
    `Investigator notes: ${caseData.notes || ''}`,
    `Resolution notes: ${caseData.resolution_notes || ''}`,
    `Linked tags: ${tags.join(', ')}`,
    `Linked equipment: ${equipment.join(', ')}`,
  ].filter(Boolean).join('\n');
}

function renderCaseAssistantTranscript(caseData) {
  const session = getCaseAssistantSession(caseData?.case_id);
  if (!session.turns.length) {
    return '<div class="cases-empty" style="padding:0">Ask the investigator assistant to inspect this case using the same tools as the troubleshooting agent.</div>';
  }
  return session.turns.map((turn) => {
    const toolCallsHtml = turn.toolCalls?.length
      ? `<div class="case-assistant-tool-calls" data-case-assistant-tools="${escapeHtml(turn.streamId || '')}">${turn.toolCalls.map((tool) => `<span class="tool-call-chip">${escapeHtml(tool)}</span>`).join('')}</div>`
      : '';
    const responseText = turn.error || turn.response || (turn.pending ? 'Working...' : '');
    const responseClass = turn.error ? ' error' : '';
    return `
      <div class="case-assistant-turn" data-case-assistant-turn="${escapeHtml(turn.streamId || '')}">
        <div class="case-assistant-user">${escapeHtml(turn.question || '')}</div>
        ${toolCallsHtml}
        <div class="case-assistant-response${responseClass}" data-case-assistant-response="${escapeHtml(turn.streamId || '')}">${escapeHtml(responseText || '')}</div>
      </div>
    `;
  }).join('');
}

function summarizeAssistantResponse(text, maxLen = 320) {
  const normalized = String(text || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  if (normalized.length <= maxLen) return normalized;
  const truncated = normalized.slice(0, maxLen);
  const sentenceBreak = Math.max(
    truncated.lastIndexOf('. '),
    truncated.lastIndexOf('; '),
    truncated.lastIndexOf('! '),
    truncated.lastIndexOf('? ')
  );
  if (sentenceBreak > Math.floor(maxLen * 0.5)) {
    return `${truncated.slice(0, sentenceBreak + 1).trim()}`;
  }
  return `${truncated.trim()}...`;
}

function buildCaseAssistantNarrativeSummary(caseId) {
  const session = getCaseAssistantSession(caseId);
  const completedTurns = (session.turns || []).filter((turn) => !turn.pending && !turn.error && (turn.response || '').trim());
  if (!completedTurns.length) return '';

  const uniqueTools = [...new Set(
    completedTurns.flatMap((turn) => Array.isArray(turn.toolCalls) ? turn.toolCalls : [])
  )];

  const lines = ['Investigator assistant summary:'];
  completedTurns.forEach((turn, index) => {
    lines.push(`${index + 1}. ${turn.question}`);
    lines.push(`   Findings: ${summarizeAssistantResponse(turn.response)}`);
    if (turn.toolCalls?.length) {
      lines.push(`   Tools used: ${[...new Set(turn.toolCalls)].join(', ')}`);
    }
  });

  if (uniqueTools.length) {
    lines.push(`Overall tools referenced: ${uniqueTools.join(', ')}`);
  }

  return lines.join('\n');
}

function appendAssistantSummaryToNarrative() {
  const caseId = casesState.currentCase?.case_id;
  if (!caseId) return;
  const explanationInput = document.getElementById('case-explanation-input');
  if (!explanationInput) return;
  const summary = buildCaseAssistantNarrativeSummary(caseId);
  if (!summary) return;
  const existing = explanationInput.value.trim();
  explanationInput.value = existing ? `${existing}\n\n${summary}` : summary;
  explanationInput.focus();
  explanationInput.selectionStart = explanationInput.selectionEnd = explanationInput.value.length;
}

function getCaseAssistantTranscriptElement() {
  return document.querySelector('.case-assistant-transcript');
}

function escapeForAttribute(value) {
  return CSS.escape(String(value || ''));
}

function ensureCaseAssistantTurnDom(turn) {
  const transcript = getCaseAssistantTranscriptElement();
  if (!transcript || !turn?.streamId) return null;
  const selector = `[data-case-assistant-turn="${escapeForAttribute(turn.streamId)}"]`;
  let turnEl = transcript.querySelector(selector);
  if (turnEl) return turnEl;

  const userText = escapeHtml(turn.question || '');
  turnEl = document.createElement('div');
  turnEl.className = 'case-assistant-turn';
  turnEl.setAttribute('data-case-assistant-turn', turn.streamId);
  turnEl.innerHTML = `
    <div class="case-assistant-user">${userText}</div>
    <div class="case-assistant-response" data-case-assistant-response="${escapeHtml(turn.streamId)}">${escapeHtml(turn.pending ? 'Working...' : (turn.response || ''))}</div>
  `;
  transcript.appendChild(turnEl);
  transcript.scrollTop = transcript.scrollHeight;
  return turnEl;
}

function appendCaseAssistantToolCallDom(streamId, tool) {
  const turnEl = document.querySelector(`[data-case-assistant-turn="${escapeForAttribute(streamId)}"]`);
  if (!turnEl) return;
  let toolsEl = turnEl.querySelector(`[data-case-assistant-tools="${escapeForAttribute(streamId)}"]`);
  if (!toolsEl) {
    toolsEl = document.createElement('div');
    toolsEl.className = 'case-assistant-tool-calls';
    toolsEl.setAttribute('data-case-assistant-tools', streamId);
    const responseEl = turnEl.querySelector(`[data-case-assistant-response="${escapeForAttribute(streamId)}"]`);
    if (responseEl) {
      turnEl.insertBefore(toolsEl, responseEl);
    } else {
      turnEl.appendChild(toolsEl);
    }
  }
  const chip = document.createElement('span');
  chip.className = 'tool-call-chip';
  chip.textContent = String(tool || 'tool');
  toolsEl.appendChild(chip);
  const transcript = getCaseAssistantTranscriptElement();
  if (transcript) transcript.scrollTop = transcript.scrollHeight;
}

function appendCaseAssistantResponseChunkDom(streamId, text) {
  const responseEl = document.querySelector(`[data-case-assistant-response="${escapeForAttribute(streamId)}"]`);
  if (!responseEl) return;
  if (responseEl.textContent === 'Working...') responseEl.textContent = '';
  responseEl.textContent += text || '';
  responseEl.classList.remove('error');
  const transcript = getCaseAssistantTranscriptElement();
  if (transcript) transcript.scrollTop = transcript.scrollHeight;
}

function finalizeCaseAssistantTurnDom(streamId, options = {}) {
  const responseEl = document.querySelector(`[data-case-assistant-response="${escapeForAttribute(streamId)}"]`);
  if (!responseEl) return;
  if (options.error) {
    responseEl.textContent = options.error;
    responseEl.classList.add('error');
  } else if (!responseEl.textContent.trim()) {
    responseEl.textContent = 'Done.';
  }
}

function ensureCaseAssistantListeners() {
  if (casesState.assistantListenersReady) return;
  casesState.assistantListenersReady = true;

  const findTurnByStreamId = (streamId) => {
    for (const session of Object.values(casesState.assistantSessions)) {
      const turn = session.turns.find((item) => item.streamId === streamId);
      if (turn) return turn;
    }
    return null;
  };

  window.api.onToolCall((data) => {
    if (data?.target !== 'cases-assistant' || !data.streamId) return;
    const turn = findTurnByStreamId(data.streamId);
    if (!turn) return;
    turn.toolCalls = turn.toolCalls || [];
    turn.toolCalls.push(String(data.tool || 'tool'));
    appendCaseAssistantToolCallDom(data.streamId, data.tool);
  });

  window.api.onStreamOutput((data) => {
    if (data?.target !== 'cases-assistant' || !data.streamId || data.type !== 'claude-stream') return;
    const turn = findTurnByStreamId(data.streamId);
    if (!turn) return;
    turn.response = `${turn.response || ''}${data.text || ''}`;
    appendCaseAssistantResponseChunkDom(data.streamId, data.text || '');
  });

  window.api.onStreamComplete((data) => {
    if (data?.target !== 'cases-assistant' || !data.streamId) return;
    const turn = findTurnByStreamId(data.streamId);
    if (!turn) return;
    turn.pending = false;
    finalizeCaseAssistantTurnDom(data.streamId);
  });
}

function normalizeCaseStatus(status) {
  const value = String(status || 'open').toLowerCase();
  if (value === 'in review') return 'in_review';
  return value;
}

function formatCaseDate(ts) {
  if (!ts) return 'n/a';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return String(ts);
  return d.toLocaleString();
}

function parseCaseListValue(value) {
  if (Array.isArray(value)) return value.filter(Boolean).map(String);
  if (typeof value === 'string' && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.filter(Boolean).map(String);
    } catch {
      return [value];
    }
  }
  return [];
}

function parseCaseJsonObject(value) {
  if (value && typeof value === 'object') return value;
  if (typeof value === 'string' && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === 'object') return parsed;
    } catch {
      return {};
    }
  }
  return {};
}

function updateCasesToolbar() {
  const el = getCasesElements();
  const selected = casesState.currentCase;
  const actionState = casesState.actionState;
  const actionPending = actionState?.tone === 'pending';
  if (el.countLabel) {
    const count = casesState.cases.length;
    el.countLabel.textContent = `${count} case${count === 1 ? '' : 's'}`;
  }
  if (el.btnRefresh) {
    el.btnRefresh.disabled = casesState.isLoadingList || casesState.isLoadingDetail || actionPending;
    el.btnRefresh.textContent = casesState.isLoadingList ? 'Refreshing...' : 'Refresh';
  }
  if (el.btnGenerate) {
    el.btnGenerate.disabled = !selected || casesState.isLoadingList || casesState.isLoadingDetail || actionPending;
    el.btnGenerate.textContent = actionState?.action === 'generate-report'
      ? (actionState.buttonLabel || 'Generating...')
      : 'Generate Report';
  }
  if (el.btnSaveReport) {
    el.btnSaveReport.disabled = !casesState.currentReport || casesState.isLoadingList || casesState.isLoadingDetail || actionPending;
    el.btnSaveReport.textContent = actionState?.action === 'save-report'
      ? (actionState.buttonLabel || 'Saving...')
      : 'Save Report';
  }

  let chipText = 'Cases';
  let statusText = 'Select a case or create one from the Agents tab.';
  let tone = '';

  if (casesState.statusOverride) {
    chipText = casesState.statusOverride.chip;
    statusText = casesState.statusOverride.text;
    tone = casesState.statusOverride.tone || '';
  } else if (casesState.isLoadingDetail && casesState.selectedCaseId) {
    chipText = 'Loading';
    statusText = `Loading ${casesState.selectedCaseId}...`;
    tone = 'pending';
  } else if (casesState.isLoadingList) {
    chipText = 'Loading';
    statusText = 'Loading investigations...';
    tone = 'pending';
  } else if (selected) {
    chipText = String(selected.status || 'open');
    statusText = `Selected ${selected.case_id || ''}`;
    if (String(selected.status || '').toLowerCase() === 'closed') {
      tone = 'running';
    }
  }

  if (el.statusChip) {
    el.statusChip.className = 'status-chip';
    if (tone) el.statusChip.classList.add(tone);
    el.statusChip.textContent = chipText;
  }
  if (el.statusText) {
    el.statusText.textContent = statusText;
  }
}

function renderCaseList() {
  const el = getCasesElements();
  if (!el.list) return;
  if (casesState.isLoadingList && !casesState.cases.length) {
    el.list.innerHTML = '<div class="cases-empty cases-empty-loading">Loading investigations...</div>';
    updateCasesToolbar();
    return;
  }
  if (!casesState.cases.length) {
    el.list.innerHTML = '<div class="cases-empty">No investigation cases yet. Promote an anomaly from the Agents tab to start one.</div>';
    updateCasesToolbar();
    return;
  }

  el.list.innerHTML = casesState.cases.map((item) => {
    const selectedClass = item.case_id === casesState.selectedCaseId ? ' selected' : '';
    const status = normalizeCaseStatus(item.status);
    return `
      <div class="case-list-item${selectedClass}" data-case-id="${escapeHtml(item.case_id || '')}">
        <div class="case-list-topline">
          <span class="case-list-title" title="${escapeHtml(item.title || item.case_id || '')}">${escapeHtml(item.title || item.case_id || 'Untitled case')}</span>
          <span class="case-pill status-${escapeHtml(status)}">${escapeHtml(status.replace('_', ' '))}</span>
        </div>
        <div class="case-list-meta">
          <span>${escapeHtml(item.severity || 'unknown')} severity</span>
          <span>${escapeHtml(item.subsystem_name || item.subsystem_id || 'unscoped')}</span>
        </div>
      </div>
    `;
  }).join('');

  el.list.querySelectorAll('.case-list-item').forEach((node) => {
    node.addEventListener('click', async () => {
      const caseId = node.getAttribute('data-case-id');
      if (caseId) await loadCaseDetails(caseId);
    });
  });

  updateCasesToolbar();
}

function renderCaseDetail() {
  const el = getCasesElements();
  if (!el.detail) return;

  if (casesState.isLoadingDetail) {
    el.detail.innerHTML = '<div class="cases-empty cases-empty-detail">Loading case details...</div>';
    updateCasesToolbar();
    return;
  }

  if (!casesState.currentCase) {
    el.detail.innerHTML = '<div class="cases-empty cases-empty-detail">Choose a case to inspect the event context, capture notes, and generate a report.</div>';
    updateCasesToolbar();
    return;
  }

  const item = casesState.currentCase;
  const event = item.event || {};
  const probableCauses = parseCaseListValue(item.probable_causes_json || event.probable_causes_json);
  const recommendedChecks = parseCaseListValue(item.recommended_checks_json || event.recommended_checks_json);
  const draftCauses = parseCaseListValue(item.draft_probable_causes_json);
  const draftChecks = parseCaseListValue(item.draft_recommended_checks_json);
  const draftContext = parseCaseJsonObject(item.draft_context_json);
  const tags = Array.isArray(item.tags) ? item.tags : [];
  const equipment = Array.isArray(item.equipment) ? item.equipment : [];
  const draftStatus = String(item.draft_status || '').toLowerCase();
  const hasDraft = Boolean(item.draft_summary || item.draft_explanation || draftCauses.length || draftChecks.length);
  const actionState = casesState.actionState;
  const actionPending = actionState?.tone === 'pending';
  const assistantSession = getCaseAssistantSession(item.case_id);
  const hasAssistantTranscript = assistantSession.turns.some((turn) => !turn.pending && ((turn.response || '').trim() || (turn.error || '').trim()));
  const dependencySummary = [
    ...(draftContext.upstream_views || []).map((name) => `Upstream: ${name}`),
    ...(draftContext.downstream_views || []).map((name) => `Downstream: ${name}`),
  ];
  const reportHtml = casesState.currentReport
    ? `<pre class="case-report-output">${escapeHtml(casesState.currentReport.markdown || '')}</pre>`
    : '<div class="cases-empty" style="padding:0">Generate a report to create a shareable post-incident narrative.</div>';
  const saveLabel = actionState?.action === 'save-case' ? (actionState.buttonLabel || 'Saving...') : 'Save Case';
  const closeLabel = actionState?.action === 'close-case' ? (actionState.buttonLabel || 'Closing...') : 'Close Case';
  const deleteLabel = actionState?.action === 'delete-case' ? (actionState.buttonLabel || 'Deleting...') : 'Delete Case';
  const draftLabel = actionState?.action === 'generate-draft' ? (actionState.buttonLabel || 'Generating...') : 'AI Enrich Draft';
  const reportLabel = actionState?.action === 'generate-report' ? (actionState.buttonLabel || 'Generating...') : 'Generate Report';
  const approveLabel = actionState?.action === 'approve-draft' ? (actionState.buttonLabel || 'Approving...') : 'Approve Draft';
  const rejectLabel = actionState?.action === 'reject-draft' ? (actionState.buttonLabel || 'Rejecting...') : 'Reject Draft';
  const regenerateLabel = actionState?.action === 'generate-draft' ? (actionState.buttonLabel || 'Generating...') : 'Regenerate Draft';
  const actionStatusHtml = actionState?.message
    ? `<div class="case-action-status tone-${escapeHtml(actionState.tone || 'pending')}">${escapeHtml(actionState.message)}</div>`
    : '';

  el.detail.innerHTML = `
    <div class="case-detail-header">
      <div class="case-detail-title-block">
        <h3>${escapeHtml(item.title || 'Untitled case')}</h3>
        <div class="case-detail-subtitle">Case ID <code>${escapeHtml(item.case_id || '')}</code> linked to event <code>${escapeHtml(item.source_event_id || 'n/a')}</code></div>
      </div>
      <span class="case-pill status-${escapeHtml(normalizeCaseStatus(item.status))}">${escapeHtml(normalizeCaseStatus(item.status).replace('_', ' '))}</span>
    </div>

    <div class="case-detail-grid">
      <div class="case-detail-stat"><span class="case-detail-stat-label">Subsystem</span><span class="case-detail-stat-value">${escapeHtml(item.subsystem_name || item.subsystem_id || 'Unknown')}</span></div>
      <div class="case-detail-stat"><span class="case-detail-stat-label">Source Tag</span><span class="case-detail-stat-value">${escapeHtml(item.source_tag || event.source_tag || 'n/a')}</span></div>
      <div class="case-detail-stat"><span class="case-detail-stat-label">Severity</span><span class="case-detail-stat-value">${escapeHtml(item.severity || 'unknown')}</span></div>
      <div class="case-detail-stat"><span class="case-detail-stat-label">Last Updated</span><span class="case-detail-stat-value">${escapeHtml(formatCaseDate(item.updated_at))}</span></div>
    </div>

    <div class="case-form-grid">
      <div class="case-form-field">
        <label for="case-status-input">Status</label>
        <select class="input" id="case-status-input">
          <option value="open"${normalizeCaseStatus(item.status) === 'open' ? ' selected' : ''}>Open</option>
          <option value="in_review"${normalizeCaseStatus(item.status) === 'in_review' ? ' selected' : ''}>In Review</option>
          <option value="closed"${normalizeCaseStatus(item.status) === 'closed' ? ' selected' : ''}>Closed</option>
        </select>
      </div>
      <div class="case-form-field">
        <label for="case-owner-input">Owner</label>
        <input class="input" id="case-owner-input" value="${escapeHtml(item.owner || '')}" placeholder="Responsible operator or engineer">
      </div>
      <div class="case-form-field">
        <label for="case-disposition-input">Disposition</label>
        <input class="input" id="case-disposition-input" value="${escapeHtml(item.disposition || '')}" placeholder="Awaiting root cause, mitigated, resolved...">
      </div>
      <div class="case-form-field">
        <label for="case-summary-input">Working Summary</label>
        <input class="input" id="case-summary-input" value="${escapeHtml(item.summary || '')}" placeholder="Short investigation headline">
      </div>
      <div class="case-form-field case-form-field-full">
        <label for="case-explanation-input">Investigation Narrative</label>
        <textarea class="input" id="case-explanation-input" rows="4" placeholder="What happened and why does it matter?">${escapeHtml(item.explanation || '')}</textarea>
      </div>
      <div class="case-form-field case-form-field-full">
        <label for="case-operator-context-input">Operator Context</label>
        <textarea class="input" id="case-operator-context-input" rows="3" placeholder="Observed symptoms, attempted actions, timeline, and production impact...">${escapeHtml(item.operator_context || '')}</textarea>
      </div>
      <div class="case-form-field case-form-field-full">
        <label for="case-notes-input">Operator Notes</label>
        <textarea class="input" id="case-notes-input" rows="4" placeholder="Evidence, timeline, observations...">${escapeHtml(item.notes || '')}</textarea>
      </div>
      <div class="case-form-field case-form-field-full">
        <label for="case-resolution-input">Resolution Notes</label>
        <textarea class="input" id="case-resolution-input" rows="4" placeholder="Corrective action, reset steps, remaining follow-ups...">${escapeHtml(item.resolution_notes || '')}</textarea>
      </div>
    </div>

    <div class="case-form-actions">
      <button class="btn btn-primary" id="btn-case-save"${actionPending ? ' disabled' : ''}>${escapeHtml(saveLabel)}</button>
      <button class="btn btn-secondary" id="btn-case-close"${actionPending || normalizeCaseStatus(item.status) === 'closed' ? ' disabled' : ''}>${escapeHtml(closeLabel)}</button>
      <button class="btn btn-secondary" id="btn-case-generate-draft"${actionPending ? ' disabled' : ''}>${escapeHtml(draftLabel)}</button>
      <button class="btn btn-secondary" id="btn-case-generate-report"${actionPending ? ' disabled' : ''}>${escapeHtml(reportLabel)}</button>
      <button class="btn btn-danger" id="btn-case-delete"${actionPending ? ' disabled' : ''}>${escapeHtml(deleteLabel)}</button>
    </div>
    ${actionStatusHtml}

    ${hasDraft ? `
      <div class="case-draft-panel">
        <div class="case-draft-header">
          <h4>AI Draft</h4>
          <span class="case-pill status-${escapeHtml(draftStatus || 'open')}">${escapeHtml((draftStatus || 'pending_approval').replace('_', ' '))}</span>
        </div>
        ${item.draft_summary ? `<div class="case-draft-summary">${escapeHtml(item.draft_summary)}</div>` : ''}
        ${item.draft_explanation ? `<div class="case-draft-narrative">${escapeHtml(item.draft_explanation)}</div>` : ''}
        ${dependencySummary.length ? `<div class="case-draft-deps">${dependencySummary.map((x) => `<span class="case-dependency-chip">${escapeHtml(x)}</span>`).join('')}</div>` : ''}
        <div class="case-linked-lists">
          <div class="case-linked-panel">
            <h4>Draft Probable Causes</h4>
            ${draftCauses.length ? `<ul>${draftCauses.map((x) => `<li>${escapeHtml(x)}</li>`).join('')}</ul>` : '<div class="cases-empty" style="padding:0">No draft probable causes.</div>'}
          </div>
          <div class="case-linked-panel">
            <h4>Draft Recommended Checks</h4>
            ${draftChecks.length ? `<ul>${draftChecks.map((x) => `<li>${escapeHtml(x)}</li>`).join('')}</ul>` : '<div class="cases-empty" style="padding:0">No draft checks.</div>'}
          </div>
        </div>
        <div class="case-form-actions">
          <button class="btn btn-primary" id="btn-case-approve-draft"${actionPending ? ' disabled' : ''}>${escapeHtml(approveLabel)}</button>
          <button class="btn btn-ghost" id="btn-case-reject-draft"${actionPending ? ' disabled' : ''}>${escapeHtml(rejectLabel)}</button>
          <button class="btn btn-secondary" id="btn-case-regenerate-draft"${actionPending ? ' disabled' : ''}>${escapeHtml(regenerateLabel)}</button>
        </div>
      </div>
    ` : ''}

    <div class="case-linked-lists">
      <div class="case-linked-panel">
        <h4>Probable Causes</h4>
        ${probableCauses.length ? `<ul>${probableCauses.map((x) => `<li>${escapeHtml(x)}</li>`).join('')}</ul>` : '<div class="cases-empty" style="padding:0">No probable causes recorded yet.</div>'}
      </div>
      <div class="case-linked-panel">
        <h4>Recommended Checks</h4>
        ${recommendedChecks.length ? `<ul>${recommendedChecks.map((x) => `<li>${escapeHtml(x)}</li>`).join('')}</ul>` : '<div class="cases-empty" style="padding:0">No recommended checks recorded yet.</div>'}
      </div>
      <div class="case-linked-panel">
        <h4>Linked Tags</h4>
        ${tags.length ? `<ul>${tags.map((x) => `<li><code>${escapeHtml(x)}</code></li>`).join('')}</ul>` : '<div class="cases-empty" style="padding:0">No linked tags.</div>'}
      </div>
      <div class="case-linked-panel">
        <h4>Linked Equipment</h4>
        ${equipment.length ? `<ul>${equipment.map((x) => `<li><code>${escapeHtml(x)}</code></li>`).join('')}</ul>` : '<div class="cases-empty" style="padding:0">No linked equipment.</div>'}
      </div>
    </div>

    <div class="case-report-panel">
      <h4>Generated Report</h4>
      ${reportHtml}
    </div>

    <div class="case-report-panel">
      <h4>Investigator Assistant</h4>
      <div class="case-assistant-transcript">${renderCaseAssistantTranscript(item)}</div>
      <div class="case-assistant-input-row">
        <input class="input" id="case-assistant-input" placeholder="Ask about this case using the full query-agent toolset">
        <button class="btn btn-secondary" id="btn-case-assistant-clear">Clear Chat</button>
        <button class="btn btn-secondary" id="btn-case-assistant-summary"${!hasAssistantTranscript || actionPending ? ' disabled' : ''}>Append Summary to Narrative</button>
        <button class="btn btn-primary" id="btn-case-assistant-send"${actionPending ? ' disabled' : ''}>Ask Assistant</button>
      </div>
    </div>
  `;

  document.getElementById('btn-case-save')?.addEventListener('click', saveSelectedCase);
  document.getElementById('btn-case-close')?.addEventListener('click', closeSelectedCase);
  document.getElementById('btn-case-generate-draft')?.addEventListener('click', generateSelectedCaseDraft);
  document.getElementById('btn-case-generate-report')?.addEventListener('click', generateSelectedCaseReport);
  document.getElementById('btn-case-delete')?.addEventListener('click', deleteSelectedCase);
  document.getElementById('btn-case-approve-draft')?.addEventListener('click', approveSelectedCaseDraft);
  document.getElementById('btn-case-reject-draft')?.addEventListener('click', rejectSelectedCaseDraft);
  document.getElementById('btn-case-regenerate-draft')?.addEventListener('click', generateSelectedCaseDraft);
  document.getElementById('btn-case-assistant-send')?.addEventListener('click', sendCaseAssistantQuery);
  document.getElementById('btn-case-assistant-summary')?.addEventListener('click', appendAssistantSummaryToNarrative);
  document.getElementById('btn-case-assistant-input')?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendCaseAssistantQuery();
    }
  });
  document.getElementById('btn-case-assistant-clear')?.addEventListener('click', clearCaseAssistantSession);

  const caseDetailRoot = el.detail;
  caseDetailRoot.querySelectorAll('input, textarea, select').forEach((field) => {
    field.addEventListener('pointerdown', (event) => {
      const hit = document.elementFromPoint(event.clientX, event.clientY);
      appendCasesUiDebugLog(
        `pointerdown target=${field.id || field.tagName} hit=${hit?.id || hit?.className || hit?.tagName || 'unknown'} actionPending=${casesState.actionState?.tone === 'pending'}`
      );
    });
    field.addEventListener('focusin', () => {
      appendCasesUiDebugLog(`focusin target=${field.id || field.tagName}`);
    });
  });
  caseDetailRoot.addEventListener('pointerdown', (event) => {
    const hit = document.elementFromPoint(event.clientX, event.clientY);
    if (!hit) return;
    const targetLabel = event.target?.id || event.target?.className || event.target?.tagName || 'unknown';
    const hitLabel = hit.id || hit.className || hit.tagName || 'unknown';
    if (targetLabel !== hitLabel) {
      appendCasesUiDebugLog(`detail pointerdown target=${targetLabel} hit=${hitLabel}`);
    }
  });

  updateCasesToolbar();
}

function clearCaseAssistantSession() {
  const caseId = casesState.currentCase?.case_id;
  if (!caseId) return;
  casesState.assistantSessions[caseId] = {
    history: [],
    turns: [],
  };
  renderCaseDetail();
}

async function sendCaseAssistantQuery() {
  const caseId = casesState.currentCase?.case_id;
  if (!caseId) return;
  const input = document.getElementById('case-assistant-input');
  const question = input?.value?.trim() || '';
  if (!question) return;

  const session = getCaseAssistantSession(caseId);
  const stream = createCaseStreamContext(`INVESTIGATOR QUERY ${caseId}`, 'cases-assistant');
  const turn = {
    streamId: stream.streamId,
    question,
    response: '',
    toolCalls: [],
    pending: true,
    error: '',
  };
  session.turns.push(turn);
  if (input) input.value = '';
  renderCaseDetail();
  ensureCaseAssistantTurnDom(turn);

  const result = await window.api.casesAssistantQuery(
    question,
    session.history,
    buildCaseAssistantContext(casesState.currentCase),
    stream,
  );

  turn.pending = false;
  if (!result.success) {
    turn.error = result.error || 'Investigator assistant query failed';
    finalizeCaseAssistantTurnDom(stream.streamId, { error: turn.error });
    return;
  }

  session.history = Array.isArray(result.history) ? result.history : session.history;
  turn.response = result.response || turn.response || '';
  finalizeCaseAssistantTurnDom(stream.streamId);
}

async function loadCaseDetails(caseId) {
  const requestSeq = ++casesState.detailRequestSeq;
  const stream = createCaseStreamContext(`LOAD CASE ${caseId}`);
  casesState.selectedCaseId = caseId;
  casesState.currentCase = null;
  casesState.currentReport = null;
  casesState.isLoadingDetail = true;
  renderCaseList();
  renderCaseDetail();
  const result = await window.api.casesGet(caseId, stream);
  if (requestSeq !== casesState.detailRequestSeq) return;
  casesState.isLoadingDetail = false;
  if (!result.success || !result.case) {
    completeCaseStream(stream.streamId, false);
    setCasesStatusOverride('Error', result.error || `Failed to load ${caseId}`, 'error');
    renderCaseDetail();
    return;
  }
  casesState.selectedCaseId = caseId;
  casesState.currentCase = result.case;
  casesState.currentReport = null;
  clearCasesStatusOverride();
  renderCaseList();
  renderCaseDetail();
}

async function loadCases(preferredCaseId = null) {
  const el = getCasesElements();
  const requestSeq = ++casesState.listRequestSeq;
  const stream = createCaseStreamContext('LOAD CASE LIST');
  casesState.isLoadingList = true;
  renderCaseList();
  const result = await window.api.casesList({
    limit: 100,
    status: el.filterStatus?.value || undefined,
  }, stream);
  if (requestSeq !== casesState.listRequestSeq) return;
  casesState.isLoadingList = false;
  if (!result.success) {
    completeCaseStream(stream.streamId, false);
    setCasesStatusOverride('Error', result.error || 'Failed to load investigations', 'error');
    if (el.list && !casesState.cases.length) {
      el.list.innerHTML = `<div class="cases-empty">Failed to load cases: ${escapeHtml(result.error || 'Unknown error')}</div>`;
    }
    updateCasesToolbar();
    return;
  }

  casesState.cases = Array.isArray(result.cases) ? result.cases : [];
  renderCaseList();

  const nextCaseId = preferredCaseId || casesState.selectedCaseId;
  if (nextCaseId && casesState.cases.some((item) => item.case_id === nextCaseId)) {
    await loadCaseDetails(nextCaseId);
    return;
  }

  casesState.selectedCaseId = null;
  casesState.currentCase = null;
  casesState.currentReport = null;
  clearCasesStatusOverride();
  renderCaseDetail();
}

async function createCaseFromAgentEvent(eventId, btnEl) {
  const event = agentsState.events.find((item) => item.event_id === eventId);
  if (!event) return;
  const stream = createCaseStreamContext(`CREATE CASE ${eventId}`);
  setAgentEventActionStatus(eventId, 'create-case', 'Creating investigation case...', {
    buttonLabel: 'Creating...',
  });
  casesState.selectedCaseId = null;
  casesState.currentCase = null;
  casesState.currentReport = null;
  casesState.isLoadingDetail = true;
  activateTab('cases');
  if (!casesState.initialized) initCasesTab();
  renderCaseDetail();
  setCasesStatusOverride('Opening', 'Creating investigation case...', 'pending');
  const result = await window.api.casesCreateFromEvent(event, stream);
  if (!result.success || !result.case) {
    completeCaseStream(stream.streamId, false);
    casesState.isLoadingDetail = false;
    const errorText = result.error || 'Failed to create investigation case';
    setAgentEventActionStatus(eventId, 'create-case', errorText, {
      tone: 'error',
      buttonLabel: 'Retry',
    });
    setCasesStatusOverride('Error', errorText, 'error');
    return;
  }
  setAgentEventActionStatus(eventId, 'create-case', 'Opening case workspace...', {
    buttonLabel: 'Opening...',
  });
  setCasesStatusOverride('Opening', `Opening ${result.case.case_id || 'new case'}...`, 'pending');
  casesState.selectedCaseId = result.case.case_id;
  casesState.currentCase = null;
  casesState.currentReport = null;
  casesState.isLoadingDetail = false;
  await loadCases(result.case.case_id);
  const draftStream = createCaseStreamContext(`AUTO AI ENRICH ${result.case.case_id}`);
  setCaseActionState('generate-draft', 'Generating AI draft for the new case...', {
    buttonLabel: 'Generating...',
    statusChip: 'Working',
    statusText: 'Generating AI draft for the new case...',
  });
  const draftResult = await window.api.casesGenerateDraft(result.case.case_id, draftStream);
  if (!draftResult.success || !draftResult.case) {
    completeCaseStream(draftStream.streamId, false);
    setCaseActionState('generate-draft', draftResult.error || 'Failed to generate AI draft', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: draftResult.error || 'Failed to generate AI draft',
    });
    clearAgentEventActionStatus(eventId);
    return;
  }
  casesState.currentCase = draftResult.case;
  casesState.selectedCaseId = draftResult.case.case_id;
  await loadCases(draftResult.case.case_id);
  clearCaseActionState();
  clearAgentEventActionStatus(eventId);
}

async function aiEnrichEvent(eventId, btnEl) {
  const event = agentsState.events.find((item) => item.event_id === eventId);
  if (!event) return;
  const createStream = createCaseStreamContext(`AI ENRICH CREATE CASE ${eventId}`);
  agentsState.pendingDeepAnalyze.add(eventId);
  let clearActionStatusOnExit = true;
  setAgentEventActionStatus(eventId, 'ai-enrich', 'Creating case for AI draft...', {
    buttonLabel: 'Creating...',
  });
  setCasesStatusOverride('Opening', 'Creating case for AI draft...', 'pending');
  renderSubsystemHealthGrid();
  try {
    const caseResult = await window.api.casesCreateFromEvent(event, createStream);
    if (!caseResult.success || !caseResult.case) {
      completeCaseStream(createStream.streamId, false);
      const errorText = caseResult.error || 'Failed to create case for AI draft';
      setAgentEventActionStatus(eventId, 'ai-enrich', errorText, {
        tone: 'error',
        buttonLabel: 'Retry',
      });
      setCasesStatusOverride('Error', errorText, 'error');
      clearActionStatusOnExit = false;
      return;
    }
    setAgentEventActionStatus(eventId, 'ai-enrich', 'Generating AI draft...', {
      buttonLabel: 'Generating...',
    });
    setCasesStatusOverride('Opening', 'Generating AI draft...', 'pending');
    const draftStream = createCaseStreamContext(`AI ENRICH DRAFT ${caseResult.case.case_id}`);
    const draftResult = await window.api.casesGenerateDraft(caseResult.case.case_id, draftStream);
    if (!draftResult.success) {
      completeCaseStream(draftStream.streamId, false);
      setAgentEventActionStatus(eventId, 'ai-enrich', 'AI draft failed. Opening raw case instead...', {
        tone: 'error',
        buttonLabel: 'Retry',
      });
      setCasesStatusOverride('Opening', 'AI draft failed, opening raw case...', 'pending');
    } else {
      setAgentEventActionStatus(eventId, 'ai-enrich', 'Opening AI draft in Cases...', {
        buttonLabel: 'Opening...',
      });
      setCasesStatusOverride('Opening', 'Opening AI draft in Cases...', 'pending');
    }
    casesState.selectedCaseId = caseResult.case.case_id;
    casesState.currentCase = draftResult.case || caseResult.case;
    casesState.currentReport = null;
    activateTab('cases');
    if (!casesState.initialized) initCasesTab();
    await loadCases(caseResult.case.case_id);
  } finally {
    agentsState.pendingDeepAnalyze.delete(eventId);
    if (clearActionStatusOnExit) clearAgentEventActionStatus(eventId);
    renderSubsystemHealthGrid();
  }
}

async function generateSelectedCaseDraft() {
  if (!casesState.currentCase?.case_id) return;
  const stream = createCaseStreamContext(`GENERATE DRAFT ${casesState.currentCase.case_id}`);
  setCaseActionState('generate-draft', 'Generating AI draft for this case...', {
    buttonLabel: 'Generating...',
    statusChip: 'Working',
    statusText: 'Generating AI draft...',
  });
  const result = await window.api.casesGenerateDraft(casesState.currentCase.case_id, stream);
  if (!result.success || !result.case) {
    completeCaseStream(stream.streamId, false);
    setCaseActionState('generate-draft', result.error || 'Failed to generate AI draft', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to generate AI draft',
    });
    return;
  }
  casesState.currentCase = result.case;
  casesState.selectedCaseId = result.case.case_id;
  casesState.currentReport = null;
  await loadCases(result.case.case_id);
  clearCaseActionState();
}

async function approveSelectedCaseDraft() {
  if (!casesState.currentCase?.case_id) return;
  const stream = createCaseStreamContext(`APPROVE DRAFT ${casesState.currentCase.case_id}`);
  setCaseActionState('approve-draft', 'Approving AI draft and merging it into the case...', {
    buttonLabel: 'Approving...',
    statusChip: 'Working',
    statusText: 'Approving AI draft...',
  });
  const result = await window.api.casesApproveDraft(casesState.currentCase.case_id, stream);
  if (!result.success || !result.case) {
    completeCaseStream(stream.streamId, false);
    setCaseActionState('approve-draft', result.error || 'Failed to approve AI draft', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to approve AI draft',
    });
    return;
  }
  casesState.currentCase = result.case;
  casesState.selectedCaseId = result.case.case_id;
  await loadCases(result.case.case_id);
  clearCaseActionState();
}

async function rejectSelectedCaseDraft() {
  if (!casesState.currentCase?.case_id) return;
  const stream = createCaseStreamContext(`REJECT DRAFT ${casesState.currentCase.case_id}`);
  setCaseActionState('reject-draft', 'Rejecting AI draft...', {
    buttonLabel: 'Rejecting...',
    statusChip: 'Working',
    statusText: 'Rejecting AI draft...',
  });
  const result = await window.api.casesRejectDraft(casesState.currentCase.case_id, stream);
  if (!result.success || !result.case) {
    completeCaseStream(stream.streamId, false);
    setCaseActionState('reject-draft', result.error || 'Failed to reject AI draft', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to reject AI draft',
    });
    return;
  }
  casesState.currentCase = result.case;
  casesState.selectedCaseId = result.case.case_id;
  await loadCases(result.case.case_id);
  clearCaseActionState();
}

async function saveSelectedCase() {
  if (!casesState.currentCase?.case_id) return;
  const stream = createCaseStreamContext(`SAVE CASE ${casesState.currentCase.case_id}`);
  setCaseActionState('save-case', 'Saving case updates...', {
    buttonLabel: 'Saving...',
    statusChip: 'Working',
    statusText: 'Saving case updates...',
  });
  const patch = {
    status: document.getElementById('case-status-input')?.value || 'open',
    owner: document.getElementById('case-owner-input')?.value || '',
    disposition: document.getElementById('case-disposition-input')?.value || '',
    summary: document.getElementById('case-summary-input')?.value || '',
    explanation: document.getElementById('case-explanation-input')?.value || '',
    operator_context: document.getElementById('case-operator-context-input')?.value || '',
    notes: document.getElementById('case-notes-input')?.value || '',
    resolution_notes: document.getElementById('case-resolution-input')?.value || '',
  };
  const result = await window.api.casesUpdate(casesState.currentCase.case_id, patch, stream);
  if (!result.success || !result.case) {
    completeCaseStream(stream.streamId, false);
    setCaseActionState('save-case', result.error || 'Failed to save case', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to save case',
    });
    return;
  }
  casesState.currentCase = result.case;
  casesState.selectedCaseId = result.case.case_id;
  casesState.currentReport = null;
  await loadCases(result.case.case_id);
  clearCaseActionState();
}

async function closeSelectedCase() {
  if (!casesState.currentCase?.case_id) return;
  const caseId = casesState.currentCase.case_id;
  const stream = createCaseStreamContext(`CLOSE CASE ${caseId}`);
  setCaseActionState('close-case', 'Closing case...', {
    buttonLabel: 'Closing...',
    statusChip: 'Working',
    statusText: 'Closing case...',
  });
  const result = await window.api.casesUpdate(caseId, { status: 'closed' }, stream);
  if (!result.success || !result.case) {
    completeCaseStream(stream.streamId, false);
    setCaseActionState('close-case', result.error || 'Failed to close case', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to close case',
    });
    return;
  }
  casesState.currentCase = result.case;
  casesState.selectedCaseId = result.case.case_id;
  casesState.currentReport = null;
  await loadCases(result.case.case_id);
  clearCaseActionState();
}

async function deleteSelectedCase() {
  if (!casesState.currentCase?.case_id) return;
  const caseId = casesState.currentCase.case_id;
  const confirmed = window.confirm(`Delete case ${caseId}? This cannot be undone.`);
  if (!confirmed) return;

  const stream = createCaseStreamContext(`DELETE CASE ${caseId}`);
  setCaseActionState('delete-case', 'Deleting case...', {
    buttonLabel: 'Deleting...',
    statusChip: 'Working',
    statusText: 'Deleting case...',
  });
  const result = await window.api.casesDelete(caseId, stream);
  if (!result.success) {
    completeCaseStream(stream.streamId, false);
    setCaseActionState('delete-case', result.error || 'Failed to delete case', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to delete case',
    });
    return;
  }

  delete casesState.assistantSessions[caseId];
  casesState.selectedCaseId = null;
  casesState.currentCase = null;
  casesState.currentReport = null;
  clearCasesStatusOverride();
  clearCaseActionState();
  await loadCases(null);
}

async function generateSelectedCaseReport() {
  if (!casesState.currentCase?.case_id) return;
  const stream = createCaseStreamContext(`GENERATE REPORT ${casesState.currentCase.case_id}`);
  setCaseActionState('generate-report', 'Generating investigation report...', {
    buttonLabel: 'Generating...',
    statusChip: 'Working',
    statusText: 'Generating investigation report...',
  });
  const result = await window.api.casesGenerateReport(casesState.currentCase.case_id, stream);
  if (!result.success) {
    completeCaseStream(stream.streamId, false);
    setCaseActionState('generate-report', result.error || 'Failed to generate report', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to generate report',
    });
    return;
  }
  casesState.currentReport = {
    markdown: result.markdown || '',
    filename: result.filename || `investigation_${casesState.currentCase.case_id}.md`,
  };
  if (result.case) {
    casesState.currentCase = result.case;
  }
  clearCasesStatusOverride();
  clearCaseActionState();
  renderCaseDetail();
}

async function saveSelectedCaseReport() {
  if (!casesState.currentReport) return;
  setCaseActionState('save-report', `Saving ${casesState.currentReport.filename || 'report'}...`, {
    buttonLabel: 'Saving...',
    statusChip: 'Working',
    statusText: 'Saving generated report...',
  });
  const result = await window.api.casesSaveReport(casesState.currentReport.filename, casesState.currentReport.markdown);
  if (result && result.success === false) {
    setCaseActionState('save-report', result.error || 'Failed to save report', {
      tone: 'error',
      buttonLabel: 'Retry',
      statusChip: 'Error',
      statusText: result.error || 'Failed to save report',
    });
    return;
  }
  clearCasesStatusOverride();
  clearCaseActionState();
}

function initCasesTab() {
  const el = getCasesElements();
  ensureCasesLogListeners();
  ensureCaseAssistantListeners();
  if (!el.list || casesState.initialized) {
    if (el.list) loadCases(casesState.selectedCaseId);
    return;
  }

  casesState.initialized = true;
  el.btnClearLog?.addEventListener('click', () => appendCasesLog('', true));
  el.btnRefresh?.addEventListener('click', () => loadCases(casesState.selectedCaseId));
  el.filterStatus?.addEventListener('change', () => loadCases(null));
  el.btnGenerate?.addEventListener('click', generateSelectedCaseReport);
  el.btnSaveReport?.addEventListener('click', saveSelectedCaseReport);
  loadCases(casesState.selectedCaseId);
}

function ensureAgentListeners() {
  if (agentsState.listenersReady) return;
  agentsState.listenersReady = true;

  window.api.onAgentStatus((payload) => {
    if (!payload) return;
    if (payload.runId) agentsState.runId = payload.runId;
    agentsState.status = payload.state || agentsState.status;
    updateAgentStatusUi(agentsState.status, `Run ${agentsState.runId || 'n/a'}`);

    const diagnostics = payload.diagnostics || {};
    const phase = diagnostics.phase || '?';
    const subId = payload.subsystemId || diagnostics.subsystemId || '';
    if (phase === 'triage_slow_candidate') {
      console.warn(`[Agent ${subId}] SLOW TRIAGE: ${diagnostics.tag} use_llm=${diagnostics.use_llm} llm=${diagnostics.llm_ms}ms persist=${diagnostics.persist_ms}ms total=${diagnostics.total_ms}ms`);
    } else if (phase === 'cycle_complete' && subId) {
      const t = diagnostics.timingMs || {};
      console.log(`[Agent ${subId}] cycle #${diagnostics.cycleCount || '?'} ${payload.cycleMs || diagnostics.avgCycleMs || 0}ms (read=${t.read || '?'}ms hist=${t.history || '?'}ms score=${t.score || '?'}ms triage=${t.triage || '?'}ms) ${payload.candidates || 0} cand`);
    } else if (phase === 'agents_started' || phase === 'rediscovery_complete') {
      console.log(`[Agents] ${phase}: ${diagnostics.agentCount || 0} agents`);
    }

    updateSubsystemHealthFromStatus(payload);
  });

  window.api.onAgentEvent((payload) => upsertRealtimeAgentEvent(payload));

  window.api.onAgentError((payload) => {
    if (!payload) return;
    console.error('[Agents error]', payload.code, payload.message);
    if (!payload.recoverable) updateAgentStatusUi('error', payload.message || 'Agent runtime error');
  });

  window.api.onAgentComplete((payload) => {
    if (!payload) return;
    console.log('[Agents] run complete, success=' + payload.success);
    agentsState.status = payload.success ? 'stopped' : 'failed';
    updateAgentStatusUi(agentsState.status, payload.reason || 'Run complete');
  });
}

function initAgentsTab() {
  ensureAgentListeners();
  const el = getAgentsElements();
  if (!el.btnStart) return;
  if (!el.btnStart.dataset.bound) {
    el.btnStart.dataset.bound = '1';
    el.btnStart.addEventListener('click', startAgentsMonitoring);
    el.btnStop?.addEventListener('click', stopAgentsMonitoring);
    el.btnRefresh?.addEventListener('click', loadAgentEvents);
    el.btnCleanup?.addEventListener('click', async () => {
      await window.api.agentsCleanup(14);
      await loadAgentEvents();
    });
    el.filterState?.addEventListener('change', () => renderSubsystemHealthGrid());
    el.filterSeverity?.addEventListener('change', () => renderSubsystemHealthGrid());
    el.filterSearch?.addEventListener('input', () => renderSubsystemHealthGrid());
    const clearSubBtn = document.getElementById('btn-agents-clear-subsystem');
    clearSubBtn?.addEventListener('click', () => {
      agentsState.selectedSubsystemId = null;
      agentsState.selectedEventId = null;
      clearSubBtn.style.display = 'none';
      renderSubsystemHealthGrid();
    });
  }
  refreshAgentStatus();
  loadAgentEvents();
  renderSubsystemHealthGrid();
}

// Initialize graph tab when it's first shown
navButtons.forEach(btn => {
  btn.addEventListener('click', () => {
    if (btn.dataset.tab === 'graph') {
      // Slight delay to ensure DOM is ready
      setTimeout(initGraphTab, 100);
    }
    if (btn.dataset.tab === 'browse') {
      // Refresh browse data when switching to tab
      loadProjects();
      loadGatewayResources();
      loadTiaProjects();
    }
    if (btn.dataset.tab === 'dexpi') {
      // Initialize DEXPI tab on first visit
      setTimeout(initDexpiTab, 100);
    }
    if (btn.dataset.tab === 'settings') {
      loadSettings();
      loadDbConnections();
    }
    if (btn.dataset.tab === 'agents') {
      setTimeout(initAgentsTab, 100);
    }
    if (btn.dataset.tab === 'cases') {
      setCasesStatusOverride('Loading', casesState.initialized ? 'Refreshing investigations...' : 'Loading investigations...', 'pending');
      setTimeout(initCasesTab, 100);
    }
  });
});

// Check connection and load stats on startup
setTimeout(() => {
  updateStats();
  loadProjects();
  loadGatewayResources();
  loadTiaProjects();
  loadSettings();
  loadDbConnections();
  ensureAgentListeners();
  initCasesTab();
}, 500);

