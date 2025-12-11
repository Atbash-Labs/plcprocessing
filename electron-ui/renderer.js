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
  // Clean up Neo4j deprecation warnings
  const cleaned = text
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
  document.getElementById('btn-run-unified').disabled = disabled;
  document.getElementById('btn-run-enrichment').disabled = disabled;
}

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

// Ignition Ingest (with streaming - no blocking overlay)
document.getElementById('btn-ingest-ignition').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  appendOutput(`\n📊 Ingesting: ${filePath}\n`, false);
  appendOutput('⏳ Processing...\n');
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.ingestIgnition(filePath);
    
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

// Troubleshooting Enrichment (with streaming - no blocking overlay)
document.getElementById('btn-run-enrichment').addEventListener('click', async () => {
  appendOutput('\n🔧 Running troubleshooting enrichment...\n', false);
  appendOutput('⏳ Processing...\n');
  isIngestActive = true;
  disableIngestButtons(true);
  
  try {
    const result = await window.api.runEnrichment();
    
    if (result.success) {
      appendOutput('\n✅ Troubleshooting enrichment complete!\n');
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
  // Clean up Neo4j warnings first
  text = text
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
      const cleaned = result.output
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

// Clear Database
document.getElementById('btn-clear-db').addEventListener('click', async () => {
  const confirmed = confirm('⚠️ This will DELETE ALL DATA from the ontology database.\n\nAre you sure you want to continue?');
  if (!confirmed) return;
  
  showLoading('Clearing database...');
  
  try {
    const result = await window.api.clearDatabase();
    if (result.success) {
      alert('✅ Database cleared successfully!');
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
setTimeout(updateStats, 500);

