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

// PLC Ingest
document.getElementById('btn-ingest-plc').addEventListener('click', async () => {
  const filePath = await window.api.selectFile({
    filters: [
      { name: 'PLC Files', extensions: ['sc', 'L5X'] },
      { name: 'All Files', extensions: ['*'] }
    ]
  });
  
  if (!filePath) return;
  
  // Don't use loading overlay - we want to see streaming output
  appendOutput(`\n[INGEST] ${filePath}\n`, false);
  
  try {
    const result = await window.api.ingestPLC(filePath);
    if (result.success) {
      // Don't re-append result.output since it's already streamed
      appendOutput('\n[OK] PLC ingestion complete!\n');
    } else {
      appendOutput(`\n[ERROR] ${result.error}\n`);
    }
  } catch (error) {
    appendOutput(`\n[ERROR] ${error.message}\n`);
  }
  
  updateStats();
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
    
    el.textContent = item[labelField] || item.name || 'Unknown';
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
// Initial Load
// ============================================

// Initialize Browse tab
initBrowseSubTabs();

// Check connection and load stats on startup
setTimeout(() => {
  updateStats();
  loadProjects();
  loadGatewayResources();
}, 500);

