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

// ============================================
// Ingest Tab Handlers
// ============================================

// PLC Ingest
document.getElementById('btn-ingest-plc').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'PLC Files', extensions: ['sc', 'L5X'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  showLoading('Analyzing PLC file...');
  appendOutput(`\n📥 Ingesting: ${filePath}\n`, false);
  
  try {
    const result = await window.api.ingestPLC(filePath);
    if (result.success) {
      appendOutput(result.output);
      appendOutput('\n✅ PLC ingestion complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  hideLoading();
  updateStats();
});

// Ignition Ingest
document.getElementById('btn-ingest-ignition').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'Ignition Backup', extensions: ['json'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  showLoading('Analyzing Ignition backup...');
  appendOutput(`\n📊 Ingesting: ${filePath}\n`, false);
  
  try {
    const result = await window.api.ingestIgnition(filePath);
    if (result.success) {
      appendOutput(result.output);
      appendOutput('\n✅ Ignition ingestion complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  hideLoading();
  updateStats();
});

// Unified Analysis
document.getElementById('btn-run-unified').addEventListener('click', async () => {
  showLoading('Running unified analysis (linking PLC ↔ SCADA)...');
  appendOutput('\n🔗 Running unified analysis...\n', false);
  
  try {
    const result = await window.api.runUnified();
    if (result.success) {
      appendOutput(result.output);
      appendOutput('\n✅ Unified analysis complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  hideLoading();
  updateStats();
});

// Troubleshooting Enrichment
document.getElementById('btn-run-enrichment').addEventListener('click', async () => {
  showLoading('Adding troubleshooting data...');
  appendOutput('\n🔧 Running troubleshooting enrichment...\n', false);
  
  try {
    const result = await window.api.runEnrichment();
    if (result.success) {
      appendOutput(result.output);
      appendOutput('\n✅ Troubleshooting enrichment complete!\n');
    } else {
      appendOutput(`\n❌ Error: ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n❌ Error: ${error.message}\n`);
  }
  
  hideLoading();
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

async function sendMessage() {
  const question = chatInput.value.trim();
  if (!question) return;
  
  // Add user message to UI
  addMessage(question, true);
  chatInput.value = '';
  
  // Show thinking indicator
  const thinkingDiv = document.createElement('div');
  thinkingDiv.className = 'message assistant';
  thinkingDiv.innerHTML = `
    <div class="message-content">
      <p>🤔 Analyzing your question...</p>
    </div>
  `;
  chatMessages.appendChild(thinkingDiv);
  chatMessages.scrollTop = chatMessages.scrollHeight;
  
  try {
    // Send question with full conversation history
    const result = await window.api.troubleshoot(question, conversationHistory);
    
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
      addMessage(`❌ Error: ${result.error}`, false);
    }
  } catch (error) {
    chatMessages.removeChild(thinkingDiv);
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

