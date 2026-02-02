/**
 * Graph Renderer - Cytoscape.js wrapper for ontology visualization
 * Reusable component for both Graph Tab (editable) and Modal (read-only)
 */

// Register Cytoscape extensions (must be done once before use)
if (typeof cytoscape !== 'undefined') {
  // Register dagre layout extension
  if (typeof cytoscapeDagre !== 'undefined') {
    cytoscape.use(cytoscapeDagre);
  }
  // Register fcose layout extension
  if (typeof cytoscapeFcose !== 'undefined') {
    cytoscape.use(cytoscapeFcose);
  }
}

class GraphRenderer {
  /**
   * Create a new GraphRenderer
   * @param {HTMLElement} container - Container element for the graph
   * @param {Object} options - Configuration options
   * @param {boolean} options.editable - Enable editing mode (default: false)
   * @param {string} options.layout - Initial layout: 'force' or 'hierarchical' (default: 'force')
   * @param {Function} options.onNodeSelect - Callback when node is selected
   * @param {Function} options.onEdgeSelect - Callback when edge is selected
   * @param {Function} options.onNodeCreate - Callback when node is created (edit mode only)
   * @param {Function} options.onEdgeCreate - Callback when edge is created (edit mode only)
   */
  constructor(container, options = {}) {
    this.container = container;
    this.options = {
      editable: false,
      layout: 'force',
      onNodeSelect: null,
      onEdgeSelect: null,
      onNodeCreate: null,
      onEdgeCreate: null,
      ...options
    };
    
    this.cy = null;
    this.currentLayout = this.options.layout;
    this.pendingChanges = {
      nodes: { create: [], update: [], delete: [] },
      edges: { create: [], update: [], delete: [] }
    };
    
    // Hierarchical layer ordering (bottom to top for dagre)
    this.layerOrder = {
      'plc': 0,
      'scada': 1,
      'mes': 2,
      'troubleshooting': 3,
      'flows': 4,
      'overview': 5,
      'other': 6
    };
    
    this._initCytoscape();
  }
  
  /**
   * Initialize Cytoscape instance
   */
  _initCytoscape() {
    this.cy = cytoscape({
      container: this.container,
      
      style: [
        // Node styles
        {
          selector: 'node',
          style: {
            'label': 'data(label)',
            'text-valign': 'bottom',
            'text-halign': 'center',
            'text-margin-y': 8,
            'font-size': 11,
            'font-family': 'Inter, sans-serif',
            'color': '#e8e8f0',
            'text-outline-width': 2,
            'text-outline-color': '#0a0a0f',
            'background-color': 'data(color)',
            'width': 40,
            'height': 40,
            'border-width': 2,
            'border-color': '#2a2a3a'
          }
        },
        // Center node (for neighbor view)
        {
          selector: 'node[?isCenter]',
          style: {
            'width': 50,
            'height': 50,
            'border-width': 3,
            'border-color': '#00d4ff',
            'font-weight': 600
          }
        },
        // Selected node
        {
          selector: 'node:selected',
          style: {
            'border-width': 3,
            'border-color': '#00d4ff',
            'background-opacity': 1
          }
        },
        // Pending create (dashed border)
        {
          selector: 'node.pending-create',
          style: {
            'border-style': 'dashed',
            'border-color': '#00ff88',
            'border-width': 3
          }
        },
        // Pending delete (red overlay)
        {
          selector: 'node.pending-delete',
          style: {
            'opacity': 0.5,
            'border-color': '#ff4466',
            'border-width': 3
          }
        },
        // Pending update (yellow highlight)
        {
          selector: 'node.pending-update',
          style: {
            'border-color': '#ff9944',
            'border-width': 3
          }
        },
        
        // Edge styles
        {
          selector: 'edge',
          style: {
            'width': 2,
            'line-color': '#3a3a4a',
            'target-arrow-color': '#3a3a4a',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            'label': 'data(label)',
            'font-size': 9,
            'color': '#8888a0',
            'text-rotation': 'autorotate',
            'text-margin-y': -10,
            'text-outline-width': 2,
            'text-outline-color': '#0a0a0f'
          }
        },
        // Selected edge
        {
          selector: 'edge:selected',
          style: {
            'width': 3,
            'line-color': '#00d4ff',
            'target-arrow-color': '#00d4ff'
          }
        },
        // Pending create edge
        {
          selector: 'edge.pending-create',
          style: {
            'line-style': 'dashed',
            'line-color': '#00ff88',
            'target-arrow-color': '#00ff88'
          }
        },
        // Pending delete edge
        {
          selector: 'edge.pending-delete',
          style: {
            'opacity': 0.5,
            'line-color': '#ff4466',
            'target-arrow-color': '#ff4466'
          }
        },
        
        // Dimmed elements (for filtering)
        {
          selector: '.dimmed',
          style: {
            'opacity': 0.15
          }
        }
      ],
      
      layout: { name: 'preset' },
      
      minZoom: 0.1,
      maxZoom: 3,
      wheelSensitivity: 0.3,
      
      // Selection behavior
      boxSelectionEnabled: this.options.editable,
      selectionType: 'single'
    });
    
    this._setupEventHandlers();
  }
  
  /**
   * Set up event handlers
   */
  _setupEventHandlers() {
    // Node selection
    this.cy.on('tap', 'node', (evt) => {
      const node = evt.target;
      if (this.options.onNodeSelect) {
        // Pass the original event so shift-click can be detected
        this.options.onNodeSelect(node.data(), evt.originalEvent);
      }
    });
    
    // Edge selection
    this.cy.on('tap', 'edge', (evt) => {
      const edge = evt.target;
      if (this.options.onEdgeSelect) {
        this.options.onEdgeSelect(edge.data());
      }
    });
    
    // Double-click to expand neighbors
    this.cy.on('dblclick', 'node', (evt) => {
      const node = evt.target;
      if (this.onNodeDoubleClick) {
        this.onNodeDoubleClick(node.data());
      }
    });
    
    // Right-click context menu (edit mode)
    if (this.options.editable) {
      this.cy.on('cxttap', (evt) => {
        const position = evt.position || evt.cyPosition;
        if (evt.target === this.cy) {
          // Clicked on canvas - show create node menu
          if (this.onCanvasContextMenu) {
            this.onCanvasContextMenu(position);
          }
        } else if (evt.target.isNode()) {
          // Clicked on node
          if (this.onNodeContextMenu) {
            this.onNodeContextMenu(evt.target.data(), position);
          }
        } else if (evt.target.isEdge()) {
          // Clicked on edge
          if (this.onEdgeContextMenu) {
            this.onEdgeContextMenu(evt.target.data(), position);
          }
        }
      });
    }
    
    // Semantic zoom - hide labels at low zoom
    this.cy.on('zoom', () => {
      const zoom = this.cy.zoom();
      if (zoom < 0.5) {
        this.cy.style().selector('node').style('label', '').update();
        this.cy.style().selector('edge').style('label', '').update();
      } else {
        this.cy.style().selector('node').style('label', 'data(label)').update();
        this.cy.style().selector('edge').style('label', 'data(label)').update();
      }
    });
  }
  
  /**
   * Load graph data
   * @param {Object} data - Graph data with nodes and edges arrays
   */
  loadData(data) {
    const elements = [];
    
    // Add nodes
    if (data.nodes) {
      for (const node of data.nodes) {
        elements.push({
          data: {
            id: node.id,
            label: this._truncateLabel(node.label, 25),
            fullLabel: node.label,
            type: node.type,
            group: node.group,
            color: node.color || '#9E9E9E',
            isCenter: node.isCenter || false,
            distance: node.distance || 0,
            properties: node.properties || {}
          }
        });
      }
    }
    
    // Add edges
    if (data.edges) {
      for (const edge of data.edges) {
        elements.push({
          data: {
            id: edge.id,
            source: edge.source,
            target: edge.target,
            type: edge.type,
            label: edge.label || edge.type,
            properties: edge.properties || {}
          }
        });
      }
    }
    
    this.cy.elements().remove();
    this.cy.add(elements);
    this.runLayout();
  }
  
  /**
   * Truncate label for display
   */
  _truncateLabel(label, maxLength) {
    if (!label) return '';
    if (label.length <= maxLength) return label;
    return label.substring(0, maxLength - 3) + '...';
  }
  
  /**
   * Run layout algorithm
   * @param {string} layoutType - 'force' or 'hierarchical'
   */
  runLayout(layoutType = null) {
    if (layoutType) {
      this.currentLayout = layoutType;
    }
    
    let layoutConfig;
    
    if (this.currentLayout === 'hierarchical') {
      // Dagre hierarchical layout (if available)
      if (typeof cytoscapeDagre !== 'undefined') {
        layoutConfig = {
          name: 'dagre',
          rankDir: 'BT', // Bottom to top (PLC at bottom, MES at top)
          nodeSep: 50,
          rankSep: 80,
          edgeSep: 20,
          animate: true,
          animationDuration: 500,
          // Group nodes by layer
          rankFunction: (node) => {
            const group = node.data('group');
            return this.layerOrder[group] || 5;
          }
        };
      } else {
        // Fallback to grid layout
        layoutConfig = {
          name: 'grid',
          animate: true,
          animationDuration: 300
        };
      }
    } else {
      // fCoSE force-directed layout (if available)
      if (typeof cytoscapeFcose !== 'undefined') {
        layoutConfig = {
          name: 'fcose',
          quality: 'default',
          randomize: true,
          animate: true,
          animationDuration: 500,
          nodeDimensionsIncludeLabels: true,
          nodeRepulsion: 4500,
          idealEdgeLength: 100,
          edgeElasticity: 0.45,
          nestingFactor: 0.1,
          gravity: 0.25,
          numIter: 2500,
          tile: true,
          // Cluster by group
          packComponents: true
        };
      } else {
        // Fallback to cose layout (built-in)
        layoutConfig = {
          name: 'cose',
          animate: true,
          animationDuration: 300,
          nodeRepulsion: 400000,
          idealEdgeLength: 100
        };
      }
    }
    
    try {
      this.cy.layout(layoutConfig).run();
    } catch (e) {
      console.warn('Layout failed, falling back to cose:', e);
      this.cy.layout({ name: 'cose', animate: true }).run();
    }
  }
  
  /**
   * Switch between layouts
   * @param {string} layoutType - 'force' or 'hierarchical'
   */
  switchLayout(layoutType) {
    this.currentLayout = layoutType;
    this.runLayout();
  }
  
  /**
   * Fit view to all elements
   */
  fit() {
    this.cy.fit(undefined, 50);
  }
  
  /**
   * Center on a specific node
   * @param {string} nodeId - Node ID to center on
   */
  centerOnNode(nodeId) {
    const node = this.cy.getElementById(nodeId);
    if (node.length > 0) {
      this.cy.animate({
        center: { eles: node },
        zoom: 1.5
      }, { duration: 300 });
    }
  }
  
  /**
   * Reset zoom and pan
   */
  resetView() {
    this.cy.animate({
      fit: { padding: 50 }
    }, { duration: 300 });
  }
  
  /**
   * Zoom in
   */
  zoomIn() {
    this.cy.zoom(this.cy.zoom() * 1.2);
  }
  
  /**
   * Zoom out
   */
  zoomOut() {
    this.cy.zoom(this.cy.zoom() / 1.2);
  }
  
  /**
   * Filter nodes by type
   * @param {string} type - Node type to show (or 'all')
   */
  filterByType(type) {
    if (type === 'all') {
      this.cy.elements().removeClass('dimmed');
    } else {
      this.cy.nodes().forEach(node => {
        if (node.data('type') === type || node.data('group') === type) {
          node.removeClass('dimmed');
          node.connectedEdges().removeClass('dimmed');
        } else {
          node.addClass('dimmed');
        }
      });
    }
  }
  
  /**
   * Search nodes
   * @param {string} query - Search string
   */
  search(query) {
    if (!query) {
      this.cy.elements().removeClass('dimmed');
      return;
    }
    
    const lowerQuery = query.toLowerCase();
    
    this.cy.nodes().forEach(node => {
      const label = (node.data('fullLabel') || '').toLowerCase();
      const type = (node.data('type') || '').toLowerCase();
      
      if (label.includes(lowerQuery) || type.includes(lowerQuery)) {
        node.removeClass('dimmed');
        node.connectedEdges().removeClass('dimmed');
      } else {
        node.addClass('dimmed');
      }
    });
  }
  
  /**
   * Clear search highlighting
   */
  clearSearch() {
    this.cy.elements().removeClass('dimmed');
  }
  
  /**
   * Highlight a node and its neighbors
   * @param {string} nodeId - Node to highlight
   */
  highlightNode(nodeId) {
    const node = this.cy.getElementById(nodeId);
    if (node.length === 0) return;
    
    // Dim all elements
    this.cy.elements().addClass('dimmed');
    
    // Un-dim the node and its neighborhood
    const neighborhood = node.neighborhood().add(node);
    neighborhood.removeClass('dimmed');
  }
  
  /**
   * Clear all highlighting
   */
  clearHighlight() {
    this.cy.elements().removeClass('dimmed');
  }
  
  // =========================================================================
  // Edit Mode Methods
  // =========================================================================
  
  /**
   * Add a pending node creation
   * @param {Object} nodeData - Node data
   */
  addPendingNode(nodeData) {
    const id = `pending-node-${Date.now()}`;
    
    this.cy.add({
      data: {
        id: id,
        label: nodeData.name,
        fullLabel: nodeData.name,
        type: nodeData.type.toLowerCase(),
        group: this._getGroupForType(nodeData.type),
        color: this._getColorForType(nodeData.type),
        properties: nodeData.properties || {},
        isPending: true
      },
      position: nodeData.position || { x: 400, y: 300 }
    });
    
    this.cy.getElementById(id).addClass('pending-create');
    
    this.pendingChanges.nodes.create.push({
      type: nodeData.type,
      name: nodeData.name,
      properties: nodeData.properties || {}
    });
    
    return id;
  }
  
  /**
   * Add a pending edge creation
   * @param {Object} edgeData - Edge data
   */
  addPendingEdge(edgeData) {
    const id = `pending-edge-${Date.now()}`;
    
    this.cy.add({
      data: {
        id: id,
        source: edgeData.sourceId,
        target: edgeData.targetId,
        type: edgeData.type,
        label: edgeData.type,
        isPending: true
      }
    });
    
    this.cy.getElementById(id).addClass('pending-create');
    
    this.pendingChanges.edges.create.push({
      sourceType: edgeData.sourceType,
      sourceName: edgeData.sourceName,
      targetType: edgeData.targetType,
      targetName: edgeData.targetName,
      type: edgeData.type,
      properties: edgeData.properties || {}
    });
    
    return id;
  }
  
  /**
   * Mark a node for pending deletion
   * @param {string} nodeId - Node ID
   */
  markNodeForDeletion(nodeId) {
    const node = this.cy.getElementById(nodeId);
    if (node.length === 0) return;
    
    node.addClass('pending-delete');
    
    const data = node.data();
    this.pendingChanges.nodes.delete.push({
      type: data.type,
      name: data.fullLabel || data.label
    });
  }
  
  /**
   * Mark an edge for pending deletion
   * @param {string} edgeId - Edge ID
   */
  markEdgeForDeletion(edgeId) {
    const edge = this.cy.getElementById(edgeId);
    if (edge.length === 0) return;
    
    edge.addClass('pending-delete');
    
    const data = edge.data();
    const sourceNode = this.cy.getElementById(data.source);
    const targetNode = this.cy.getElementById(data.target);
    
    this.pendingChanges.edges.delete.push({
      sourceType: sourceNode.data('type'),
      sourceName: sourceNode.data('fullLabel') || sourceNode.data('label'),
      targetType: targetNode.data('type'),
      targetName: targetNode.data('fullLabel') || targetNode.data('label'),
      type: data.type
    });
  }
  
  /**
   * Get pending changes
   * @returns {Object} Pending changes object
   */
  getPendingChanges() {
    return this.pendingChanges;
  }
  
  /**
   * Get pending changes count
   * @returns {Object} Counts by operation type
   */
  getPendingChangesCount() {
    return {
      nodesCreate: this.pendingChanges.nodes.create.length,
      nodesUpdate: this.pendingChanges.nodes.update.length,
      nodesDelete: this.pendingChanges.nodes.delete.length,
      edgesCreate: this.pendingChanges.edges.create.length,
      edgesDelete: this.pendingChanges.edges.delete.length,
      total: this.pendingChanges.nodes.create.length +
             this.pendingChanges.nodes.update.length +
             this.pendingChanges.nodes.delete.length +
             this.pendingChanges.edges.create.length +
             this.pendingChanges.edges.delete.length
    };
  }
  
  /**
   * Clear pending changes
   */
  clearPendingChanges() {
    // Remove visual indicators
    this.cy.$('.pending-create').remove();
    this.cy.$('.pending-delete').removeClass('pending-delete');
    this.cy.$('.pending-update').removeClass('pending-update');
    
    // Reset pending changes object
    this.pendingChanges = {
      nodes: { create: [], update: [], delete: [] },
      edges: { create: [], update: [], delete: [] }
    };
  }
  
  /**
   * Apply pending changes (removes visual indicators after successful apply)
   */
  commitPendingChanges() {
    // Remove pending-create class (they're now real)
    this.cy.$('.pending-create').removeClass('pending-create');
    
    // Remove elements marked for deletion
    this.cy.$('.pending-delete').remove();
    
    // Remove pending-update class
    this.cy.$('.pending-update').removeClass('pending-update');
    
    // Reset pending changes
    this.pendingChanges = {
      nodes: { create: [], update: [], delete: [] },
      edges: { create: [], update: [], delete: [] }
    };
  }
  
  /**
   * Get group for a node type
   */
  _getGroupForType(type) {
    const typeMap = {
      'AOI': 'plc', 'Tag': 'plc',
      'UDT': 'scada', 'Equipment': 'scada', 'View': 'scada',
      'Script': 'scada', 'NamedQuery': 'scada', 'Project': 'scada',
      'FaultSymptom': 'troubleshooting', 'FaultCause': 'troubleshooting',
      'Material': 'mes', 'Batch': 'mes', 'Operation': 'mes',
      'CCP': 'mes', 'ProductionOrder': 'mes'
    };
    return typeMap[type] || 'other';
  }
  
  /**
   * Get color for a node type
   */
  _getColorForType(type) {
    const colorMap = {
      'plc': '#F57C00',
      'scada': '#7B1FA2',
      'troubleshooting': '#FF5722',
      'mes': '#00897B',
      'other': '#9E9E9E'
    };
    const group = this._getGroupForType(type);
    return colorMap[group] || colorMap.other;
  }
  
  // =========================================================================
  // Export Methods
  // =========================================================================
  
  /**
   * Export graph as PNG
   * @returns {string} Data URL of PNG image
   */
  exportPNG() {
    return this.cy.png({ scale: 2, bg: '#0a0a0f' });
  }
  
  /**
   * Get current graph data
   * @returns {Object} Current nodes and edges
   */
  getData() {
    const nodes = [];
    const edges = [];
    
    this.cy.nodes().forEach(node => {
      nodes.push(node.data());
    });
    
    this.cy.edges().forEach(edge => {
      edges.push(edge.data());
    });
    
    return { nodes, edges };
  }
  
  /**
   * Destroy the renderer
   */
  destroy() {
    if (this.cy) {
      this.cy.destroy();
      this.cy = null;
    }
  }
}

// Export for use in renderer.js
if (typeof module !== 'undefined' && module.exports) {
  module.exports = GraphRenderer;
}
