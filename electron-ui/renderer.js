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
  appendOutput(`[TOOL] ${data.tool}\n`);
});

// Listen for stream completion
window.api.onStreamComplete((data) => {
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
    const result = await window.api.graphLoad({ limit: 500 });
    
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

document.getElementById('graph-filter')?.addEventListener('change', (e) => {
  if (graphRenderer) {
    graphRenderer.filterByType(e.target.value);
  }
});

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
}, 500);

