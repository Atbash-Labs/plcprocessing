#!/usr/bin/env python3
"""
GraphViz visualization generator for PLC/SCADA ontologies.
Supports L5X, Ignition, and unified ontology formats.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum


class OntologyType(Enum):
    L5X = "l5x"
    IGNITION = "ignition"
    UNIFIED = "unified"


@dataclass
class GraphConfig:
    """Configuration for graph generation."""
    rankdir: str = "TB"  # TB, LR, BT, RL
    show_tags: bool = True
    show_relationships: bool = True
    show_data_flows: bool = True
    max_tags_per_node: int = 20
    cluster_by_type: bool = True
    full_text: bool = True  # No truncation by default
    max_text_len: int = 500  # Only used if full_text=False


class OntologyGraphViz:
    """Generate GraphViz DOT files from ontology JSON."""

    # Color schemes
    COLORS = {
        'plc': {'fill': '#E3F2FD', 'border': '#1976D2', 'text': '#0D47A1'},
        'scada': {'fill': '#E8F5E9', 'border': '#388E3C', 'text': '#1B5E20'},
        'aoi': {'fill': '#FFF3E0', 'border': '#F57C00', 'text': '#E65100'},
        'udt': {'fill': '#F3E5F5', 'border': '#7B1FA2', 'text': '#4A148C'},
        'tag': {'fill': '#FFFDE7', 'border': '#FBC02D', 'text': '#F57F17'},
        'view': {'fill': '#E0F7FA', 'border': '#00ACC1', 'text': '#006064'},
        'flow': {'fill': '#FCE4EC', 'border': '#C2185B', 'text': '#880E4F'},
        'safety': {'fill': '#FFEBEE', 'border': '#D32F2F', 'text': '#B71C1C'},
        'integration': {'fill': '#E8EAF6', 'border': '#3F51B5', 'text': '#1A237E'},
    }

    def __init__(self, config: Optional[GraphConfig] = None):
        self.config = config or GraphConfig()
        self.node_counter = 0

    def _node_id(self, prefix: str = "n") -> str:
        """Generate unique node ID."""
        self.node_counter += 1
        return f"{prefix}_{self.node_counter}"

    def _sanitize(self, text: Any) -> str:
        """Sanitize text for DOT format."""
        if text is None:
            return ""
        if isinstance(text, (list, dict)):
            text = str(text)
        if not isinstance(text, str):
            text = str(text)
        return (text.replace('"', '\\"')
                    .replace('\n', '\\n')
                    .replace('<', '&lt;')
                    .replace('>', '&gt;')
                    .replace('&', '&amp;'))

    def _truncate(self, text: str, max_len: int = 500) -> str:
        """Truncate text with ellipsis (only if full_text is False)."""
        if not text:
            return ""
        text = str(text)
        if self.config.full_text:
            return text  # No truncation
        effective_max = max_len if max_len > 50 else self.config.max_text_len
        return text[:effective_max] + "..." if len(text) > effective_max else text

    def detect_type(self, data: Any) -> OntologyType:
        """Detect ontology type from JSON structure."""
        # L5X ontologies are stored as a list of AOIs
        if isinstance(data, list):
            return OntologyType.L5X
        if data.get('type') == 'unified_system_ontology':
            return OntologyType.UNIFIED
        if data.get('source') == 'ignition':
            return OntologyType.IGNITION
        if 'analysis' in data:
            return OntologyType.L5X
        return OntologyType.L5X

    def generate(self, data: Dict, output_path: Optional[str] = None) -> str:
        """Generate DOT file from ontology data."""
        ont_type = self.detect_type(data)

        if ont_type == OntologyType.UNIFIED:
            dot = self._generate_unified(data)
        elif ont_type == OntologyType.IGNITION:
            dot = self._generate_ignition(data)
        else:
            dot = self._generate_l5x(data)

        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(dot)

        return dot

    def _generate_l5x(self, data: Dict) -> str:
        """Generate DOT for L5X ontology."""
        lines = [
            'digraph L5X_Ontology {',
            f'  rankdir={self.config.rankdir};',
            '  node [shape=record, fontname="Arial", fontsize=10];',
            '  edge [fontname="Arial", fontsize=9];',
            '  compound=true;',
            '',
        ]

        # Handle list of AOIs or single ontology
        aois = data if isinstance(data, list) else [data]

        # Create AOI cluster
        lines.append('  subgraph cluster_plc {')
        lines.append('    label="PLC Layer (Rockwell L5X)";')
        lines.append(f'    style=filled; fillcolor="{self.COLORS["plc"]["fill"]}";')
        lines.append(f'    color="{self.COLORS["plc"]["border"]}";')
        lines.append('')

        aoi_nodes = {}
        all_relationships = []

        for aoi in aois:
            name = aoi.get('name', 'Unknown')
            analysis = aoi.get('analysis', {})
            purpose = self._truncate(analysis.get('purpose', ''), 60)

            node_id = self._node_id('aoi')
            aoi_nodes[name] = node_id

            # Build tag list
            tags = analysis.get('tags', {})
            tag_rows = []
            for tag_name, tag_desc in list(tags.items())[:self.config.max_tags_per_node]:
                tag_rows.append(f"{self._sanitize(tag_name)}: {self._sanitize(self._truncate(str(tag_desc), 30))}")

            if len(tags) > self.config.max_tags_per_node:
                tag_rows.append(f"... +{len(tags) - self.config.max_tags_per_node} more")

            tag_section = "\\l".join(tag_rows) + "\\l" if tag_rows else ""

            # AOI node with record shape
            label = f"{{{self._sanitize(name)}|{self._sanitize(purpose)}|{tag_section}}}"
            lines.append(f'    {node_id} [label="{label}", '
                        f'fillcolor="{self.COLORS["aoi"]["fill"]}", style=filled, '
                        f'color="{self.COLORS["aoi"]["border"]}"];')

            # Collect relationships
            for rel in analysis.get('relationships', []):
                all_relationships.append({
                    'from': rel.get('from', ''),
                    'to': rel.get('to', ''),
                    'type': rel.get('relationship_type', ''),
                    'aoi': name
                })

        lines.append('  }')
        lines.append('')

        # Add relationships as edges
        if self.config.show_relationships and all_relationships:
            lines.append('  // Relationships')
            seen_edges = set()
            for rel in all_relationships:
                # Create edge between tags (simplified - just show relationship type)
                from_tag = self._sanitize(rel['from'])
                to_tag = self._sanitize(rel['to'])
                rel_type = self._sanitize(self._truncate(rel['type'], 20))
                edge_key = f"{from_tag}->{to_tag}"
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    # Find which AOI these belong to
                    aoi_name = rel.get('aoi', '')
                    if aoi_name in aoi_nodes:
                        lines.append(f'  // {from_tag} -> {to_tag}: {rel_type}')

        lines.append('}')
        return '\n'.join(lines)

    def _generate_ignition(self, data: Dict) -> str:
        """Generate DOT for Ignition ontology."""
        lines = [
            'digraph Ignition_Ontology {',
            f'  rankdir={self.config.rankdir};',
            '  node [shape=record, fontname="Arial", fontsize=10];',
            '  edge [fontname="Arial", fontsize=9];',
            '  compound=true;',
            '',
        ]

        analysis = data.get('analysis', {})

        # UDT Cluster
        lines.append('  subgraph cluster_udts {')
        lines.append('    label="UDT Definitions";')
        lines.append(f'    style=filled; fillcolor="{self.COLORS["udt"]["fill"]}";')
        lines.append(f'    color="{self.COLORS["udt"]["border"]}";')
        lines.append('')

        udt_nodes = {}
        for udt_name, udt_purpose in analysis.get('udt_semantics', {}).items():
            node_id = self._node_id('udt')
            udt_nodes[udt_name] = node_id
            label = f"{{{self._sanitize(udt_name)}|{self._sanitize(self._truncate(udt_purpose, 50))}}}"
            lines.append(f'    {node_id} [label="{label}", '
                        f'fillcolor="{self.COLORS["udt"]["fill"]}", style=filled];')

        lines.append('  }')
        lines.append('')

        # Equipment Instances Cluster
        lines.append('  subgraph cluster_equipment {')
        lines.append('    label="Equipment Instances";')
        lines.append(f'    style=filled; fillcolor="{self.COLORS["scada"]["fill"]}";')
        lines.append(f'    color="{self.COLORS["scada"]["border"]}";')
        lines.append('')

        equip_nodes = {}
        for equip in analysis.get('equipment_instances', []):
            name = equip.get('name', 'Unknown')
            equip_type = equip.get('type', '')
            purpose = self._truncate(equip.get('purpose', ''), 40)

            node_id = self._node_id('equip')
            equip_nodes[name] = node_id
            label = f"{{{self._sanitize(name)}|Type: {self._sanitize(equip_type)}|{self._sanitize(purpose)}}}"
            lines.append(f'    {node_id} [label="{label}", '
                        f'fillcolor="{self.COLORS["tag"]["fill"]}", style=filled];')

        lines.append('  }')
        lines.append('')

        # Views Cluster
        lines.append('  subgraph cluster_views {')
        lines.append('    label="Views/Windows";')
        lines.append(f'    style=filled; fillcolor="{self.COLORS["view"]["fill"]}";')
        lines.append(f'    color="{self.COLORS["view"]["border"]}";')
        lines.append('')

        view_nodes = {}
        for view_name, view_purpose in analysis.get('view_purposes', {}).items():
            node_id = self._node_id('view')
            view_nodes[view_name] = node_id
            label = f"{{{self._sanitize(view_name)}|{self._sanitize(self._truncate(view_purpose, 50))}}}"
            lines.append(f'    {node_id} [label="{label}", '
                        f'fillcolor="{self.COLORS["view"]["fill"]}", style=filled];')

        lines.append('  }')
        lines.append('')

        # Data flows as edges
        if self.config.show_data_flows:
            lines.append('  // Data Flows')
            for flow in analysis.get('data_flows', []):
                flow_id = flow.get('flow_id', '')
                path = self._truncate(flow.get('path', ''), 60)
                lines.append(f'  // Flow: {flow_id} - {path}')

        # Connect equipment to UDTs
        lines.append('')
        lines.append('  // Equipment to UDT relationships')
        for equip in analysis.get('equipment_instances', []):
            equip_name = equip.get('name', '')
            equip_type = equip.get('type', '')
            if equip_name in equip_nodes:
                # Find matching UDT
                for udt_name in udt_nodes:
                    if equip_type in udt_name or udt_name in equip_type:
                        lines.append(f'  {udt_nodes[udt_name]} -> {equip_nodes[equip_name]} '
                                   f'[label="instance", style=dashed];')
                        break

        lines.append('}')
        return '\n'.join(lines)

    def _generate_unified(self, data: Dict) -> str:
        """Generate DOT for unified ontology."""
        lines = [
            'digraph Unified_System_Ontology {',
            f'  rankdir={self.config.rankdir};',
            '  node [shape=record, fontname="Arial", fontsize=10];',
            '  edge [fontname="Arial", fontsize=9];',
            '  compound=true;',
            '  splines=ortho;',
            '',
            '  // Title',
            '  labelloc="t";',
            '  label="Unified PLC + SCADA System Ontology";',
            '  fontsize=16;',
            '',
        ]

        ua = data.get('unified_analysis', {})

        # System Overview node
        overview = self._truncate(ua.get('system_overview', ''), 100)
        lines.append(f'  system_overview [label="{{{self._sanitize("System Overview")}|{self._sanitize(overview)}}}", '
                    f'shape=record, style="filled,bold", fillcolor="#ECEFF1"];')
        lines.append('')

        # PLC Layer Cluster
        lines.append('  subgraph cluster_plc {')
        lines.append('    label="PLC Layer (Rockwell L5X)";')
        lines.append(f'    style=filled; fillcolor="{self.COLORS["plc"]["fill"]}";')
        lines.append(f'    color="{self.COLORS["plc"]["border"]}"; penwidth=2;')
        lines.append('')

        plc_nodes = {}
        plc_ontology = data.get('component_ontologies', {}).get('plc', [])
        if isinstance(plc_ontology, list):
            for aoi in plc_ontology[:10]:  # Limit nodes
                name = aoi.get('name', 'Unknown')
                purpose = self._truncate(aoi.get('analysis', {}).get('purpose', ''), 40)
                node_id = self._node_id('plc')
                plc_nodes[name] = node_id
                label = f"{{{self._sanitize(name)}|{self._sanitize(purpose)}}}"
                lines.append(f'    {node_id} [label="{label}", '
                            f'fillcolor="{self.COLORS["aoi"]["fill"]}", style=filled];')

        lines.append('  }')
        lines.append('')

        # SCADA Layer Cluster
        lines.append('  subgraph cluster_scada {')
        lines.append('    label="SCADA Layer (Ignition)";')
        lines.append(f'    style=filled; fillcolor="{self.COLORS["scada"]["fill"]}";')
        lines.append(f'    color="{self.COLORS["scada"]["border"]}"; penwidth=2;')
        lines.append('')

        scada_nodes = {}
        scada_ontology = data.get('component_ontologies', {}).get('scada', {})
        scada_analysis = scada_ontology.get('analysis', {})

        # UDTs
        for udt_name, udt_purpose in list(scada_analysis.get('udt_semantics', {}).items())[:8]:
            node_id = self._node_id('scada')
            scada_nodes[udt_name] = node_id
            label = f"{{{self._sanitize(self._truncate(udt_name, 25))}|{self._sanitize(self._truncate(udt_purpose, 40))}}}"
            lines.append(f'    {node_id} [label="{label}", '
                        f'fillcolor="{self.COLORS["udt"]["fill"]}", style=filled];')

        # Equipment instances
        for equip in scada_analysis.get('equipment_instances', []):
            name = equip.get('name', '')
            node_id = self._node_id('equip')
            scada_nodes[name] = node_id
            label = f"{{{self._sanitize(name)}|{self._sanitize(equip.get('type', ''))}}}"
            lines.append(f'    {node_id} [label="{label}", '
                        f'fillcolor="{self.COLORS["tag"]["fill"]}", style=filled];')

        lines.append('  }')
        lines.append('')

        # PLC-to-SCADA Mappings (the key cross-system connections)
        lines.append('  // PLC-to-SCADA Mappings')
        for mapping in ua.get('plc_to_scada_mappings', []):
            plc_comp = mapping.get('plc_component', '')
            scada_comp = mapping.get('scada_component', '')
            mapping_type = self._truncate(mapping.get('mapping_type', ''), 25)

            # Find matching nodes
            plc_node = None
            scada_node = None

            for name, node_id in plc_nodes.items():
                if plc_comp in name or name in plc_comp:
                    plc_node = node_id
                    break

            for name, node_id in scada_nodes.items():
                if any(s in name or name in s for s in scada_comp.split()):
                    scada_node = node_id
                    break

            if plc_node and scada_node:
                lines.append(f'  {plc_node} -> {scada_node} '
                           f'[label="{self._sanitize(mapping_type)}", '
                           f'color="{self.COLORS["integration"]["border"]}", '
                           f'style=bold, penwidth=2];')

        lines.append('')

        # End-to-End Flows cluster
        lines.append('  subgraph cluster_flows {')
        lines.append('    label="End-to-End Data Flows";')
        lines.append(f'    style=filled; fillcolor="{self.COLORS["flow"]["fill"]}";')
        lines.append(f'    color="{self.COLORS["flow"]["border"]}";')
        lines.append('')

        for i, flow in enumerate(ua.get('end_to_end_flows', [])[:5]):
            flow_name = flow.get('flow_name', flow.get('name', f'Flow_{i}'))
            node_id = self._node_id('flow')
            label = self._sanitize(self._truncate(flow_name, 30))
            lines.append(f'    {node_id} [label="{label}", shape=ellipse, '
                        f'fillcolor="{self.COLORS["flow"]["fill"]}", style=filled];')

        lines.append('  }')
        lines.append('')

        # Safety Architecture cluster
        safety = ua.get('safety_architecture', {})
        if safety:
            lines.append('  subgraph cluster_safety {')
            lines.append('    label="Safety Architecture";')
            lines.append(f'    style=filled; fillcolor="{self.COLORS["safety"]["fill"]}";')
            lines.append(f'    color="{self.COLORS["safety"]["border"]}";')
            lines.append('')

            for layer_name in list(safety.keys())[:4]:
                node_id = self._node_id('safety')
                label = self._sanitize(layer_name.replace('_', ' ').title())
                lines.append(f'    {node_id} [label="{label}", shape=box, '
                            f'fillcolor="{self.COLORS["safety"]["fill"]}", style="filled,bold"];')

            lines.append('  }')
            lines.append('')

        # Control Responsibilities
        ctrl = ua.get('control_responsibilities', {})
        if ctrl:
            lines.append('  subgraph cluster_responsibilities {')
            lines.append('    label="Control Responsibilities";')
            lines.append('    style=filled; fillcolor="#F5F5F5";')
            lines.append('')

            for layer, funcs in ctrl.items():
                node_id = self._node_id('ctrl')
                if isinstance(funcs, dict):
                    func_list = ", ".join(list(funcs.keys())[:3])
                else:
                    func_list = str(funcs)[:50]
                label = f"{{{self._sanitize(layer)}|{self._sanitize(func_list)}}}"
                lines.append(f'    {node_id} [label="{label}", '
                            f'fillcolor="#FFFFFF", style=filled];')

            lines.append('  }')
            lines.append('')

        lines.append('}')
        return '\n'.join(lines)


def main():
    """CLI for ontology GraphViz generator."""
    parser = argparse.ArgumentParser(
        description="Generate GraphViz DOT files from ontology JSON"
    )
    parser.add_argument('input', help='Path to ontology JSON file')
    parser.add_argument('-o', '--output', help='Output DOT file path')
    parser.add_argument('-f', '--format', choices=['dot', 'svg', 'png', 'pdf'],
                       default='dot', help='Output format (default: dot)')
    parser.add_argument('--rankdir', choices=['TB', 'LR', 'BT', 'RL'],
                       default='TB', help='Graph direction (default: TB)')
    parser.add_argument('--no-tags', action='store_true',
                       help='Hide tag details')
    parser.add_argument('--no-flows', action='store_true',
                       help='Hide data flow edges')
    parser.add_argument('--truncate', action='store_true',
                       help='Truncate long text (default: show full text)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')

    args = parser.parse_args()

    # Load ontology
    with open(args.input, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Configure
    config = GraphConfig(
        rankdir=args.rankdir,
        show_tags=not args.no_tags,
        show_data_flows=not args.no_flows,
        full_text=not args.truncate
    )

    # Generate
    generator = OntologyGraphViz(config)
    ont_type = generator.detect_type(data)

    if args.verbose:
        print(f"[INFO] Detected ontology type: {ont_type.value}")

    # Determine output path
    input_path = Path(args.input)
    if args.output:
        output_path = args.output
    else:
        output_path = str(input_path.with_suffix('.dot'))

    dot_content = generator.generate(data, output_path)

    print(f"[OK] Generated DOT file: {output_path}")

    # Convert to other formats if requested
    if args.format != 'dot':
        try:
            import subprocess
            final_output = str(Path(output_path).with_suffix(f'.{args.format}'))
            result = subprocess.run(
                ['dot', f'-T{args.format}', output_path, '-o', final_output],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                print(f"[OK] Generated {args.format.upper()} file: {final_output}")
            else:
                print(f"[WARNING] GraphViz conversion failed: {result.stderr}")
                print("[INFO] Install GraphViz with: apt install graphviz (Linux) or brew install graphviz (Mac)")
        except FileNotFoundError:
            print("[WARNING] GraphViz 'dot' command not found")
            print("[INFO] Install GraphViz to convert DOT to images")
            print("[INFO] DOT file saved - you can convert manually or view at https://dreampuf.github.io/GraphvizOnline/")

    if args.verbose:
        print(f"\n[INFO] DOT content preview:\n{dot_content[:500]}...")


if __name__ == "__main__":
    main()
