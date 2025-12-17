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
// Output Panel with Streaming Support
// ============================================

const ingestOutput = document.querySelector('#ingest-output .output-content');
const toolCallsPanel = document.getElementById('tool-calls-panel');

// Track if an ingest operation is active (for streaming)
let isIngestActive = false;

function appendOutput(text, clear = false) {
  if (clear) {
    ingestOutput.textContent = '';
  }
  if (!text) return;
  // Clean up Neo4j deprecation warnings
  const cleaned = (text || '')
    .split('\n')
    .filter(line => !line.includes('GqlStatusObject') && !line.includes('Received notification'))
    .join('\n');
  ingestOutput.textContent += cleaned;
  ingestOutput.scrollTop = ingestOutput.scrollHeight;
}

function appendToolCall(toolInfo, targetElement = null) {
  const target = targetElement || ingestOutput;
  if (!target) return;
  
  // Append tool call as formatted text
  target.textContent += `  🔧 ${toolInfo}\n`;
  target.scrollTop = target.scrollHeight;
}

// Set up streaming listeners for ingest operations
window.api.onStreamOutput((data) => {
  // Only process ingest streams when an ingest operation is active
  if (!isIngestActive) return;
  if (data.streamId && data.streamId.startsWith('troubleshoot-')) return;
  
  if (data.type === 'output' || data.type === 'stderr') {
    appendOutput(data.text + '\n');
  }
});

window.api.onToolCall((data) => {
  // Only handle ingest tool calls when ingest is active
  if (!isIngestActive) return;
  if (data.streamId && data.streamId.startsWith('troubleshoot-')) return;
  
  appendToolCall(data.tool);
});

document.getElementById('btn-clear-ingest-output').addEventListener('click', () => {
  ingestOutput.textContent = '';
});

// Disable/enable ingest buttons during operations
function disableIngestButtons(disabled) {
  document.getElementById('btn-ingest-plc').disabled = disabled;
  document.getElementById('btn-ingest-ignition').disabled = disabled;
  document.getElementById('btn-ingest-ignition-incremental').disabled = disabled;
  document.getElementById('btn-run-unified').disabled = disabled;
  document.getElementById('btn-run-enrichment').disabled = disabled;
  document.getElementById('btn-run-incremental').disabled = disabled;
}

// Store last selected Ignition file for incremental analysis
let lastIgnitionFile = null;

// ============================================
// Ingest Tab Handlers
// ============================================

// PLC Ingest (with streaming - no blocking overlay)
document.getElementById('btn-ingest-plc').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    allowDirectory: true,  // Allow selecting folders of .sc files
    filters: [
      { name: 'PLC Files', extensions: ['sc', 'L5X'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  // No blocking overlay - streaming output shows progress
  appendOutput(`\n📥 Ingesting: ${filePath}\n`, false);
  appendOutput('⏳ Processing...\n');
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    // Result streams to output panel via listeners
    const result = await window.api.ingestPLC(filePath);
    
    if (result.success) {
      appendOutput('\n✅ PLC ingestion complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateStats();
});

// Ignition Ingest - Full analysis (with streaming - no blocking overlay)
document.getElementById('btn-ingest-ignition').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  lastIgnitionFile = filePath;  // Store for incremental analysis
  appendOutput(`\n📊 Ingesting: ${filePath}\n`, false);
  appendOutput('⏳ Processing with full AI analysis...\n');
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.ingestIgnition(filePath, false);
    
    if (result.success) {
      appendOutput('\n✅ Ignition ingestion complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateStats();
  updateSemanticStatus();
});

// Ignition Ingest - Import only, skip AI (for incremental analysis)
document.getElementById('btn-ingest-ignition-incremental').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  lastIgnitionFile = filePath;  // Store for incremental analysis
  appendOutput(`\n📊 Importing: ${filePath}\n`, false);
  appendOutput('⏳ Creating entities (skipping AI analysis)...\n');
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.ingestIgnition(filePath, true);
    
    if (result.success) {
      appendOutput('\n✅ Import complete! Use "Analyze Next Batch" to add semantic descriptions.\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateStats();
  updateSemanticStatus();
});

// Unified Analysis (with streaming - no blocking overlay)
document.getElementById('btn-run-unified').addEventListener('click', async () => {
  appendOutput('\n🔗 Running unified analysis (linking PLC ↔ SCADA)...\n', false);
  appendOutput('⏳ Processing...\n');
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.runUnified();
    
    if (result.success) {
      appendOutput('\n✅ Unified analysis complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateStats();
});

// Troubleshooting Enrichment for AOIs (with streaming - no blocking overlay)
document.getElementById('btn-run-enrichment').addEventListener('click', async () => {
  // Get batch size from input if available
  const batchSizeInput = document.getElementById('batch-size');
  const batchSize = batchSizeInput ? parseInt(batchSizeInput.value) || 10 : 10;
  
  appendOutput('\n🔧 Running AOI troubleshooting enrichment...\n', false);
  appendOutput(`⏳ Processing (batch size: ${batchSize})...\n`);
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.runEnrichment({ batchSize });
    
    if (result.success) {
      appendOutput('\n✅ AOI troubleshooting enrichment complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateStats();
  updateEnrichmentStatus();
});

// Troubleshooting Enrichment for Views/HMIs (with streaming)
document.getElementById('btn-run-enrichment-views').addEventListener('click', async () => {
  // Get batch size from input if available
  const batchSizeInput = document.getElementById('batch-size');
  const batchSize = batchSizeInput ? parseInt(batchSizeInput.value) || 10 : 10;
  
  appendOutput('\n🔧 Running View/HMI troubleshooting enrichment...\n', false);
  appendOutput(`⏳ Processing (batch size: ${batchSize})...\n`);
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.runEnrichmentViews({ batchSize });
    
    if (result.success) {
      appendOutput('\n✅ View troubleshooting enrichment complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateStats();
  updateEnrichmentStatus();
});

// ============================================
// Troubleshooting Enrichment Status
// ============================================

async function updateEnrichmentStatus() {
  const statusContainer = document.getElementById('enrichment-status');
  
  try {
    const result = await window.api.getEnrichmentStatus();
    
    if (result.success) {
      // Parse the output
      const output = result.output || '';
      const lines = output.split('\n').filter(l => l.trim());
      
      let html = '';
      for (const line of lines) {
        // Match lines like "  AOI             3/5 enriched (60%)"
        const match = line.match(/^\s*(\w+)\s+(\d+)\/(\d+)\s+enriched\s+\((\d+)%\)/);
        if (match) {
          const [, type, enriched, total, pct] = match;
          const isComplete = pct === '100';
          html += `
            <div class="status-item">
              <span class="type-name">${type}</span>
              <div class="progress">
                <div class="progress-bar">
                  <div class="progress-fill enriched ${isComplete ? 'complete' : ''}" style="width: ${pct}%"></div>
                </div>
                <span class="progress-text">${enriched}/${total}</span>
              </div>
            </div>
          `;
        }
      }
      
      if (html) {
        statusContainer.innerHTML = html;
      } else {
        statusContainer.innerHTML = `
          <div class="status-row">
            <span class="status-label">Status:</span>
            <span class="status-value">No items to enrich</span>
          </div>
        `;
      }
    } else {
      statusContainer.innerHTML = `
        <div class="status-row">
          <span class="status-label">Error:</span>
          <span class="status-value">${result.error}</span>
        </div>
      `;
    }
  } catch (error) {
    statusContainer.innerHTML = `
      <div class="status-row">
        <span class="status-label">Error:</span>
        <span class="status-value">${error.message}</span>
      </div>
    `;
  }
}

// Refresh enrichment status button
document.getElementById('btn-refresh-enrichment').addEventListener('click', () => {
  updateEnrichmentStatus();
});

// ============================================
// Incremental Semantic Analysis
// ============================================

// Update semantic status display
async function updateSemanticStatus() {
  const statusContainer = document.getElementById('semantic-status');
  const statusText = document.getElementById('semantic-status-text');
  
  try {
    const result = await window.api.getSemanticStatus();
    
    if (result.success) {
      // Parse the output to extract status info
      const output = result.output || '';
      const lines = output.split('\n').filter(l => l.trim());
      
      // Build status display
      let html = '';
      let totalPending = 0;
      let totalComplete = 0;
      
      let hasStuck = false;
      for (const line of lines) {
        // Match lines like "  UDT             3/5   complete (60%)"
        // Also capture optional "⚠️  X stuck in_progress" suffix
        const match = line.match(/^\s*(\w+)\s+(\d+)\/(\d+)\s+complete\s+\((\d+)%\)/);
        const stuckMatch = line.match(/(\d+)\s+stuck\s+in_progress/);
        
        if (match) {
          const [, type, complete, total, pct] = match;
          const pending = parseInt(total) - parseInt(complete);
          const stuck = stuckMatch ? parseInt(stuckMatch[1]) : 0;
          totalPending += pending;
          totalComplete += parseInt(complete);
          
          if (stuck > 0) hasStuck = true;
          
          const isComplete = pct === '100';
          html += `
            <div class="status-item">
              <span class="type-name">${type}</span>
              <div class="progress">
                <div class="progress-bar">
                  <div class="progress-fill ${isComplete ? 'complete' : ''}" style="width: ${pct}%"></div>
                </div>
                <span class="progress-text">${complete}/${total}${stuck > 0 ? ` ⚠️${stuck}` : ''}</span>
              </div>
            </div>
          `;
        }
      }
      
      if (hasStuck) {
        html += `<div class="status-warning">⚠️ Some items stuck - click "Recover Stuck"</div>`;
      }
      
      if (html) {
        statusContainer.innerHTML = html;
      } else {
        statusContainer.innerHTML = `
          <div class="status-row">
            <span class="status-label">Status:</span>
            <span class="status-value">No items found</span>
          </div>
        `;
      }
      
      // Update summary text
      if (totalPending === 0 && totalComplete > 0) {
        statusText && (statusText.textContent = '✓ All complete');
      } else if (totalPending > 0) {
        statusText && (statusText.textContent = `${totalPending} pending`);
      }
    } else {
      statusContainer.innerHTML = `
        <div class="status-row">
          <span class="status-label">Error:</span>
          <span class="status-value">${result.error}</span>
        </div>
      `;
    }
  } catch (error) {
    statusContainer.innerHTML = `
      <div class="status-row">
        <span class="status-label">Error:</span>
        <span class="status-value">${error.message}</span>
      </div>
    `;
  }
}

// Refresh semantic status button
document.getElementById('btn-refresh-semantic-status').addEventListener('click', () => {
  updateSemanticStatus();
});

// Recover stuck items button
document.getElementById('btn-recover-stuck').addEventListener('click', async () => {
  appendOutput('\n🔧 Recovering stuck items...\n', false);
  
  try {
    const result = await window.api.recoverStuck();
    if (result.success) {
      appendOutput(result.output + '\n');
      updateSemanticStatus();
    } else {
      appendOutput(`❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`❌ Error: ${error.message}\n`);
  }
});

// Run incremental analysis
document.getElementById('btn-run-incremental').addEventListener('click', async () => {
  // Check if we have a file to analyze
  if (!lastIgnitionFile) {
    // Prompt to select a file
    const filePath = await window.api.selectFile({
      filters: [
        { name: 'Ignition Backup', extensions: ['json'] },
        { name: 'All Files', extensions: ['*'] }
      ]
    });
    
    if (!filePath) {
      appendOutput('\n⚠️ No file selected. Please import an Ignition backup first.\n');
      return;
    }
    lastIgnitionFile = filePath;
  }
  
  // Get batch size from input
  const batchSizeInput = document.getElementById('batch-size');
  const batchSize = Math.max(1, Math.min(50, parseInt(batchSizeInput.value) || 10));
  batchSizeInput.value = batchSize; // Normalize the displayed value
  
  appendOutput(`\n🧠 Running incremental analysis on: ${lastIgnitionFile}\n`, false);
  appendOutput(`⏳ Analyzing next batch (up to ${batchSize} items)...\n`);
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.runIncrementalAnalysis({
      inputFile: lastIgnitionFile,
      batchSize: batchSize,
      maxItems: batchSize
    });
    
    if (result.success) {
      appendOutput('\n✅ Batch analysis complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateSemanticStatus();
});

// ============================================
// Diff Processing
// ============================================

let selectedDiffFile = null;
const diffFileDisplay = document.getElementById('diff-file-display');
const btnSelectDiff = document.getElementById('btn-select-diff');
const btnPreviewDiff = document.getElementById('btn-preview-diff');
const btnApplyDiff = document.getElementById('btn-apply-diff');

// Select diff file
btnSelectDiff.addEventListener('click', async () => {
  const filePath = await window.api.selectDiffFile();
  if (filePath) {
    selectedDiffFile = filePath;
    // Show just the filename in the display
    const fileName = filePath.split(/[/\\]/).pop();
    diffFileDisplay.innerHTML = `<span class="file-name">📋 ${fileName}</span>`;
    btnPreviewDiff.disabled = false;
    btnApplyDiff.disabled = false;
  }
});

// Preview diff changes
btnPreviewDiff.addEventListener('click', async () => {
  if (!selectedDiffFile) return;
  
  appendOutput(`\n📋 Previewing diff: ${selectedDiffFile}\n`, false);
  
  try {
    const result = await window.api.previewDiff(selectedDiffFile, lastIgnitionFile);
    if (result.success) {
      appendOutput(result.output + '\n');
    } else {
      appendOutput(`❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`❌ Error: ${error.message}\n`);
  }
});

// Apply diff changes
btnApplyDiff.addEventListener('click', async () => {
  if (!selectedDiffFile) return;
  
  const confirmed = confirm('Apply diff changes?\n\nThis will:\n• Update/create/delete entities in Neo4j\n• Mark affected entities for re-analysis\n• Cascade changes to related items');
  if (!confirmed) return;
  
  appendOutput(`\n📋 Applying diff: ${selectedDiffFile}\n`, false);
  appendOutput('⏳ Processing changes...\n');
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.applyDiff(selectedDiffFile, lastIgnitionFile);
    if (result.success) {
      appendOutput('\n✅ Diff applied successfully!\n');
      appendOutput('ℹ️ Run "Analyze Next Batch" to process pending items.\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  isIngestActive = false;
  disableIngestButtons(false);
  updateStats();
  updateSemanticStatus();
  
  // Clear the diff file selection after applying
  selectedDiffFile = null;
  diffFileDisplay.innerHTML = '<span class="placeholder">No diff file selected</span>';
  btnPreviewDiff.disabled = true;
  btnApplyDiff.disabled = true;
});

// ============================================
// Chat/Troubleshooting with Streaming Tool Calls
// ============================================

const chatMessages = document.getElementById('chat-messages');
const chatInput = document.getElementById('chat-input');
const btnSend = document.getElementById('btn-send');

// Maintain conversation history for multi-turn dialogue
let conversationHistory = [];
let isChatActive = false;
let currentToolCallsDiv = null;

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
  
  return messageDiv;
}

function createThinkingIndicator() {
  const thinkingDiv = document.createElement('div');
  thinkingDiv.className = 'message assistant thinking';
  thinkingDiv.innerHTML = `
    <div class="message-content">
      <div class="thinking-header">
        <span class="thinking-spinner">🔄</span>
        <span>Analyzing your question...</span>
      </div>
      <div class="tool-calls-container"></div>
    </div>
  `;
  return thinkingDiv;
}

function addToolCallToThinking(thinkingDiv, toolInfo) {
  const container = thinkingDiv.querySelector('.tool-calls-container');
  if (container) {
    const toolDiv = document.createElement('div');
    toolDiv.className = 'tool-call-chip';
    
    // Parse tool info to make it more readable
    let displayText = toolInfo;
    if (toolInfo.includes(':')) {
      const [toolName, ...rest] = toolInfo.split(':');
      const params = rest.join(':').trim();
      if (params.length > 60) {
        displayText = `${toolName}: ${params.substring(0, 60)}...`;
      } else {
        displayText = `${toolName}: ${params}`;
      }
    }
    
    toolDiv.innerHTML = `<span class="tool-icon">🔧</span> ${escapeHtml(displayText)}`;
    container.appendChild(toolDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
  }
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function formatResponse(text) {
  if (!text) return '';
  // Clean up Neo4j warnings first
  text = (text || '')
    .split('\n')
    .filter(line => !line.includes('GqlStatusObject') && !line.includes('Received notification'))
    .join('\n');
  
  // Basic markdown formatting
  return text
    // Headers
    .replace(/^### (.*$)/gm, '<h4>$1</h4>')
    .replace(/^## (.*$)/gm, '<h3>$1</h3>')
    .replace(/^# (.*$)/gm, '<h2>$1</h2>')
    // Bold
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    // Code blocks
    .replace(/```([\s\S]*?)```/g, '<pre>$1</pre>')
    // Inline code
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    // Horizontal rules
    .replace(/^---$/gm, '<hr>')
    // Lists
    .replace(/^- (.*$)/gm, '• $1')
    // Line breaks
    .replace(/\n/g, '<br>');
}

// Streaming listener for chat tool calls
window.api.onToolCall((data) => {
  // Only handle troubleshoot streams when chat is active
  if (data.streamId && data.streamId.startsWith('troubleshoot-')) {
    if (isChatActive && currentToolCallsDiv) {
      addToolCallToThinking(currentToolCallsDiv, data.tool);
    }
  }
});

async function sendMessage() {
  const question = chatInput.value.trim();
  if (!question) return;
  
  // Add user message to UI
  addMessage(question, true);
  chatInput.value = '';
  
  // Show thinking indicator with tool calls container
  const thinkingDiv = createThinkingIndicator();
  chatMessages.appendChild(thinkingDiv);
  chatMessages.scrollTop = chatMessages.scrollHeight;
  currentToolCallsDiv = thinkingDiv;
  isChatActive = true;
  
  try {
    // Send question with full conversation history
    const result = await window.api.troubleshoot(question, conversationHistory);
    
    // Remove thinking indicator
    if (thinkingDiv.parentNode) {
      chatMessages.removeChild(thinkingDiv);
    }
    currentToolCallsDiv = null;
    isChatActive = false;
    
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
      addMessage(`❌ Error: ${result.error}`, false);
    }
  } catch (error) {
    if (thinkingDiv.parentNode) {
      chatMessages.removeChild(thinkingDiv);
    }
    currentToolCallsDiv = null;
    isChatActive = false;
    addMessage(`❌ Error: ${error.message}`, false);
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
        <p>👋 Conversation cleared! How can I help you?</p>
      </div>
    </div>
  `;
  chatMessages.innerHTML = welcomeHtml;
});

// ============================================
// Database Tab
// ============================================

const statsDisplay = document.getElementById('stats-display');

async function updateStats() {
  try {
    const result = await window.api.getStats();
    if (result.success) {
      // Clean and format the output
      const cleaned = (result.output || '')
        .split('\n')
        .filter(line => !line.includes('GqlStatusObject') && !line.includes('Received notification'))
        .join('\n');
      statsDisplay.textContent = cleaned;
      
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

// Clear Database - All
document.getElementById('btn-clear-db').addEventListener('click', async () => {
  const confirmed = confirm('⚠️ This will DELETE ALL DATA from the ontology database.\n\nAre you sure you want to continue?');
  if (!confirmed) return;
  
  showLoading('Clearing database...');
  
  try {
    const result = await window.api.clearDatabase();
    if (result.success) {
      alert('✅ Database cleared successfully!');
      lastIgnitionFile = null;
    } else {
      alert(`❌ Error: ${result.error}`);
    }
  } catch (error) {
    alert(`❌ Error: ${error.message}`);
  }
  
  hideLoading();
  updateStats();
  updateSemanticStatus();
});

// Clear Database - Ignition only
document.getElementById('btn-clear-ignition').addEventListener('click', async () => {
  const confirmed = confirm('⚠️ This will delete all Ignition/SCADA data:\n• UDTs\n• Views\n• Equipment\n• View Components\n• Cross-system mappings\n\nPLC data will be preserved.\n\nContinue?');
  if (!confirmed) return;
  
  showLoading('Clearing Ignition data...');
  
  try {
    const result = await window.api.clearIgnition();
    if (result.success) {
      alert('✅ Ignition data cleared successfully!');
      lastIgnitionFile = null;
    } else {
      alert(`❌ Error: ${result.error}`);
    }
  } catch (error) {
    alert(`❌ Error: ${error.message}`);
  }
  
  hideLoading();
  updateStats();
  updateSemanticStatus();
});

// Clear Database - PLC only
document.getElementById('btn-clear-plc').addEventListener('click', async () => {
  const confirmed = confirm('⚠️ This will delete all PLC data:\n• AOIs\n• Tags\n• Control Patterns\n• Fault Symptoms\n• Cross-system mappings\n\nIgnition data will be preserved.\n\nContinue?');
  if (!confirmed) return;
  
  showLoading('Clearing PLC data...');
  
  try {
    const result = await window.api.clearPLC();
    if (result.success) {
      alert('✅ PLC data cleared successfully!');
    } else {
      alert(`❌ Error: ${result.error}`);
    }
  } catch (error) {
    alert(`❌ Error: ${error.message}`);
  }
  
  hideLoading();
  updateStats();
});

// Clear Database - Unification only
document.getElementById('btn-clear-unification').addEventListener('click', async () => {
  const confirmed = confirm('⚠️ This will delete all unification data:\n• PLC↔SCADA mappings\n• End-to-end flows\n• System overview\n• Operator dictionary\n\nPLC and Ignition data will be preserved.\n\nContinue?');
  if (!confirmed) return;
  
  showLoading('Clearing unification data...');
  
  try {
    const result = await window.api.clearUnification();
    if (result.success) {
      alert('✅ Unification data cleared successfully!');
    } else {
      alert(`❌ Error: ${result.error}`);
    }
  } catch (error) {
    alert(`❌ Error: ${error.message}`);
  }
  
  hideLoading();
  updateStats();
});

// Initialize Schema
document.getElementById('btn-init-db').addEventListener('click', async () => {
  showLoading('Initializing database schema...');
  
  try {
    const result = await window.api.initDatabase();
    if (result.success) {
      alert('✅ Database schema initialized!');
    } else {
      alert(`❌ Error: ${result.error}`);
    }
  } catch (error) {
    alert(`❌ Error: ${error.message}`);
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
      alert(`✅ Visualization generated!\n\nOpening: ${result.path}`);
      // Note: In a full implementation, we'd shell.openPath here
    } else {
      alert(`❌ Error: ${result.error}`);
    }
  } catch (error) {
    alert(`❌ Error: ${error.message}`);
  }
  
  hideLoading();
});

// ============================================
// Initial Load
// ============================================

// Check connection and load stats on startup
setTimeout(() => {
  updateStats();
  updateSemanticStatus();
  updateEnrichmentStatus();
}, 500);

