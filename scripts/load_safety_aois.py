#!/usr/bin/env python3
"""
Load Safety AOIs (Logic Solvers) using the existing AOI structure.

Creates AOI nodes representing Safety Instrumented System controllers
with proper tags, control patterns, and safety elements.

Usage:
    python load_safety_aois.py --load-all -v
"""

from neo4j_ontology import get_ontology_graph


def load_safety_aois(graph, verbose: bool = False):
    """Load Safety AOIs using the existing create_aoi structure."""
    
    aois = [
        # PLX Sites - Triconex systems
        {
            "name": "PLX-SLS-001",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "Schneider Electric",
                "description": "Triconex Tricon CX - Main Safety Logic Solver - Alpha Site ESD/Process Safety",
                "revision": "10.5.3",
            },
            "analysis": {
                "purpose": "SIL 3 capable safety logic solver for high pressure and high temperature protection at PLX Alpha site",
                "tags": {
                    "PT_HH_001": "High-High Pressure Trip Input",
                    "PT_HH_002": "High-High Pressure Trip Input (Redundant)",
                    "TT_HH_001": "High-High Temperature Trip Input",
                    "SDV_001_CMD": "Shutdown Valve 001 Command Output",
                    "SDV_001_FB": "Shutdown Valve 001 Position Feedback",
                    "SDV_002_CMD": "Shutdown Valve 002 Command Output",
                    "SDV_002_FB": "Shutdown Valve 002 Position Feedback",
                    "SIS_HEALTHY": "SIS System Health Status",
                    "TRIP_ACTIVE": "Trip Condition Active",
                    "BYPASS_ACTIVE": "Bypass Active Status",
                },
                "control_patterns": [
                    {"name": "2oo3_Voting", "description": "2 out of 3 voting logic for SIL 3 pressure protection"},
                    {"name": "Fail_Safe_Close", "description": "De-energize to trip shutdown valves"},
                ],
                "safety_critical": [
                    {"element": "HIPPS_Logic", "criticality": "SIL3", "reason": "High Integrity Pressure Protection"},
                    {"element": "ESD_Logic", "criticality": "SIL2", "reason": "Emergency Shutdown"},
                ],
            },
            "site": "PLX-Site-Alpha",
            "sifs": ["PLX-SIF-001", "PLX-SIF-002"],
        },
        {
            "name": "PLX-SLS-002",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "Schneider Electric",
                "description": "Triconex Tricon CX - Main Safety Logic Solver - Beta Site F&G/ESD",
                "revision": "10.5.3",
            },
            "analysis": {
                "purpose": "SIL 3 capable safety logic solver for fire & gas detection and ESD at PLX Beta site",
                "tags": {
                    "GD_001": "Gas Detector 001 Input",
                    "GD_002": "Gas Detector 002 Input",
                    "GD_003": "Gas Detector 003 Input",
                    "FD_001": "Flame Detector 001 Input",
                    "FD_002": "Flame Detector 002 Input",
                    "ESD_CMD": "Emergency Shutdown Command",
                    "DELUGE_CMD": "Deluge Valve Command",
                    "HORN_CMD": "Alarm Horn Command",
                    "BEACON_CMD": "Alarm Beacon Command",
                },
                "control_patterns": [
                    {"name": "2oo3_Gas_Voting", "description": "2 out of 3 voting for gas detection"},
                    {"name": "1oo2_Fire_Voting", "description": "1 out of 2 voting for flame detection"},
                ],
                "safety_critical": [
                    {"element": "F&G_Logic", "criticality": "SIL3", "reason": "Fire & Gas Detection System"},
                    {"element": "Deluge_Logic", "criticality": "SIL2", "reason": "Fire Suppression"},
                ],
            },
            "site": "PLX-Site-Beta",
            "sifs": ["PLX-SIF-003", "PLX-SIF-004"],
        },
        
        # MTR Sites - HIMA systems
        {
            "name": "MTR-SLS-001",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "HIMA",
                "description": "HIMA HIMax - Primary SIS Controller - Rotterdam Reactor Protection",
                "revision": "8.2.1",
            },
            "analysis": {
                "purpose": "SIL 3 safety logic solver for reactor overpressure and compressor surge protection",
                "tags": {
                    "PT_REACTOR_001": "Reactor Pressure Transmitter 1",
                    "PT_REACTOR_002": "Reactor Pressure Transmitter 2",
                    "PT_REACTOR_003": "Reactor Pressure Transmitter 3",
                    "SURGE_DETECT": "Compressor Surge Detection Signal",
                    "ASV_001_CMD": "Anti-Surge Valve Command",
                    "RV_001_CMD": "Reactor Vent Valve Command",
                    "COMP_TRIP_CMD": "Compressor Trip Command",
                },
                "control_patterns": [
                    {"name": "2oo3_Reactor_Press", "description": "TMR voting for reactor pressure"},
                    {"name": "Anti_Surge_Control", "description": "Fast-acting anti-surge protection"},
                ],
                "safety_critical": [
                    {"element": "Reactor_HIPPS", "criticality": "SIL3", "reason": "Reactor overpressure protection"},
                    {"element": "Surge_Protection", "criticality": "SIL2", "reason": "Compressor protection"},
                ],
            },
            "site": "MTR-Site-Nord",
            "sifs": ["MTR-SIF-001", "MTR-SIF-002"],
        },
        {
            "name": "MTR-SLS-002",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "HIMA",
                "description": "HIMA HIMatrix F60 - Tank Farm Safety System - Marseille",
                "revision": "5.1.2",
            },
            "analysis": {
                "purpose": "SIL 2 safety controller for tank high level protection",
                "tags": {
                    "LT_TANK_HH": "Tank High-High Level Transmitter",
                    "LSH_TANK": "Tank High Level Switch",
                    "INLET_SDV_CMD": "Tank Inlet Shutdown Valve Command",
                    "PUMP_TRIP_CMD": "Feed Pump Trip Command",
                },
                "control_patterns": [
                    {"name": "1oo2_Level", "description": "1 out of 2 voting for high level"},
                ],
                "safety_critical": [
                    {"element": "Tank_Overfill", "criticality": "SIL2", "reason": "Tank overfill prevention"},
                ],
            },
            "site": "MTR-Site-Sud",
            "sifs": ["MTR-SIF-003"],
        },
        
        # VXN Sites - Honeywell Safety Manager
        {
            "name": "VXN-SLS-001",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "Honeywell",
                "description": "Honeywell Safety Manager SC - Integrated SIS/F&G - Singapore Flare System",
                "revision": "R430.1",
            },
            "analysis": {
                "purpose": "SIL 3 integrated safety system for flare header ESD protection",
                "tags": {
                    "PT_FLARE_001": "Flare Header Pressure 1",
                    "PT_FLARE_002": "Flare Header Pressure 2",
                    "PT_FLARE_003": "Flare Header Pressure 3",
                    "FLARE_BDV_CMD": "Flare Blowdown Valve Command",
                    "FLARE_IGNITE_CMD": "Flare Ignition Command",
                },
                "control_patterns": [
                    {"name": "2oo3_Flare_Press", "description": "TMR for flare pressure protection"},
                ],
                "safety_critical": [
                    {"element": "Flare_ESD", "criticality": "SIL3", "reason": "Flare system emergency shutdown"},
                ],
            },
            "site": "VXN-Site-East",
            "sifs": ["VXN-SIF-001"],
        },
        {
            "name": "VXN-SLS-002",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "Honeywell",
                "description": "Honeywell Safety Manager SC - BMS Safety Controller - Mumbai Furnaces",
                "revision": "R430.1",
            },
            "analysis": {
                "purpose": "SIL 3 burner management system for furnace safety",
                "tags": {
                    "FLAME_UV_001": "UV Flame Scanner 1",
                    "FLAME_UV_002": "UV Flame Scanner 2",
                    "FUEL_SDV_CMD": "Fuel Gas Shutdown Valve Command",
                    "PILOT_SDV_CMD": "Pilot Gas Shutdown Valve Command",
                    "AIR_DAMPER_CMD": "Air Damper Command",
                    "PURGE_COMPLETE": "Purge Cycle Complete",
                },
                "control_patterns": [
                    {"name": "BMS_Sequence", "description": "Burner Management Sequence Control"},
                    {"name": "Flame_Monitor", "description": "Continuous flame monitoring with fail-safe"},
                ],
                "safety_critical": [
                    {"element": "BMS_Logic", "criticality": "SIL3", "reason": "Burner Management System per NFPA 86"},
                ],
            },
            "site": "VXN-Site-West",
            "sifs": ["VXN-SIF-002"],
        },
        
        # CRD Site - ABB
        {
            "name": "CRD-SLS-001",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "ABB",
                "description": "ABB AC800M HI - Emergency Blowdown & Process Safety - Rio",
                "revision": "6.1.0",
            },
            "analysis": {
                "purpose": "SIL 2 safety controller for emergency depressuring system",
                "tags": {
                    "PT_PROC_001": "Process Pressure Transmitter 1",
                    "PT_PROC_002": "Process Pressure Transmitter 2",
                    "BDV_001_CMD": "Blowdown Valve 1 Command",
                    "BDV_002_CMD": "Blowdown Valve 2 Command",
                    "EBD_INITIATE": "Emergency Blowdown Initiate",
                },
                "control_patterns": [
                    {"name": "1oo2_Blowdown", "description": "Redundant blowdown initiation"},
                ],
                "safety_critical": [
                    {"element": "EBD_Logic", "criticality": "SIL2", "reason": "Emergency Blowdown per API 521"},
                ],
            },
            "site": "CRD-Site-Rio",
            "sifs": ["CRD-SIF-001"],
        },
        
        # NVL Site - Yokogawa
        {
            "name": "NVL-SLS-001",
            "aoi_type": "SafetyLogicSolver",
            "metadata": {
                "vendor": "Yokogawa",
                "description": "Yokogawa ProSafe-RS - Offshore SIS - HIPPS & Subsea Isolation",
                "revision": "R4.05",
            },
            "analysis": {
                "purpose": "SIL 3 offshore safety system for HIPPS and subsea well isolation",
                "tags": {
                    "PT_WH_001": "Wellhead Pressure Transmitter 1",
                    "PT_WH_002": "Wellhead Pressure Transmitter 2",
                    "PT_WH_003": "Wellhead Pressure Transmitter 3",
                    "SSSV_CMD": "Subsurface Safety Valve Command",
                    "SSSV_FB": "Subsurface Safety Valve Feedback",
                    "HIPPS_SDV_CMD": "HIPPS Shutdown Valve Command",
                    "HIPPS_SDV_FB": "HIPPS Shutdown Valve Feedback",
                    "MASTER_SDV_CMD": "Master Shutdown Valve Command",
                    "WING_SDV_CMD": "Wing Valve Command",
                },
                "control_patterns": [
                    {"name": "2oo3_HIPPS", "description": "TMR voting for HIPPS protection"},
                    {"name": "Subsea_Interlock", "description": "Subsea well isolation sequence"},
                ],
                "safety_critical": [
                    {"element": "HIPPS_Logic", "criticality": "SIL3", "reason": "High Integrity Pressure Protection"},
                    {"element": "Subsea_Isolation", "criticality": "SIL3", "reason": "Subsea well isolation"},
                ],
            },
            "site": "NVL-Site-Gulf",
            "sifs": ["NVL-SIF-001", "NVL-SIF-002"],
        },
    ]
    
    with graph.session() as session:
        for aoi_data in aois:
            # Create AOI using the proper method
            graph.create_aoi(
                name=aoi_data["name"],
                aoi_type=aoi_data["aoi_type"],
                source_file="proveit_safety_system",
                metadata=aoi_data["metadata"],
                analysis=aoi_data["analysis"],
            )
            
            # Link AOI to Site
            session.run("""
                MATCH (a:AOI {name: $name})
                MATCH (s:Site {name: $site})
                MERGE (a)-[:LOCATED_AT]->(s)
            """, {"name": aoi_data["name"], "site": aoi_data["site"]})
            
            # Link AOI to SIFs it controls
            for sif_id in aoi_data.get("sifs", []):
                session.run("""
                    MATCH (a:AOI {name: $aoi})
                    MATCH (s:SIF {sif_id: $sif})
                    MERGE (a)-[:CONTROLS]->(s)
                """, {"aoi": aoi_data["name"], "sif": sif_id})
            
            if verbose:
                print(f"  Created AOI: {aoi_data['name']} with {len(aoi_data['analysis'].get('tags', []))} tags")
    
    if verbose:
        print(f"[OK] Created {len(aois)} Safety AOIs")
    
    return len(aois)


def link_aois_to_scripts(graph, verbose: bool = False):
    """Link AOIs to the event logging scripts."""
    with graph.session() as session:
        result = session.run("""
            MATCH (a:AOI)
            WHERE a.type = 'SafetyLogicSolver'
            MATCH (script:Script)
            WHERE script.name CONTAINS 'eventJournal'
            MERGE (a)-[:LOGS_TO]->(script)
            RETURN count(*) as cnt
        """)
        cnt = result.single()["cnt"]
    
    if verbose:
        print(f"[OK] Created {cnt} AOI->Script logging relationships")
    
    return cnt


def cleanup_old_plcs(graph, verbose: bool = False):
    """Remove the old PLC nodes that were created incorrectly."""
    with graph.session() as session:
        result = session.run("""
            MATCH (p:PLC)
            DETACH DELETE p
            RETURN count(p) as cnt
        """)
        cnt = result.single()["cnt"]
    
    if verbose:
        print(f"[OK] Removed {cnt} old PLC nodes")
    
    return cnt


def show_summary(graph):
    """Show summary of AOI data."""
    with graph.session() as session:
        result = session.run("""
            MATCH (a:AOI)
            WHERE a.type = 'SafetyLogicSolver'
            OPTIONAL MATCH (a)-[:LOCATED_AT]->(site:Site)
            OPTIONAL MATCH (a)-[:CONTROLS]->(sif:SIF)
            OPTIONAL MATCH (a)-[:HAS_TAG]->(tag:Tag)
            OPTIONAL MATCH (a)-[:SAFETY_CRITICAL]->(se:SafetyElement)
            RETURN a.name as aoi,
                   a.vendor as vendor,
                   site.name as site,
                   count(DISTINCT sif) as sif_count,
                   count(DISTINCT tag) as tag_count,
                   count(DISTINCT se) as safety_count
            ORDER BY a.name
        """)
        
        print("\n=== Safety Logic Solver AOIs ===")
        print(f"{'AOI':<15} {'Vendor':<20} {'Site':<20} {'SIFs':<6} {'Tags':<6} {'Safety':<6}")
        print("-" * 80)
        for r in result:
            vendor = (r['vendor'] or '')[:18]
            print(f"{r['aoi']:<15} {vendor:<20} {r['site']:<20} {r['sif_count']:<6} {r['tag_count']:<6} {r['safety_count']:<6}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Load Safety AOIs for ProveIT")
    parser.add_argument('--load-all', action='store_true',
                       help='Load all AOIs and create relationships')
    parser.add_argument('--cleanup', action='store_true',
                       help='Remove old PLC nodes')
    parser.add_argument('--summary', action='store_true',
                       help='Show AOI summary')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    graph = get_ontology_graph()
    
    try:
        if args.load_all:
            cleanup_old_plcs(graph, args.verbose)
            load_safety_aois(graph, args.verbose)
            link_aois_to_scripts(graph, args.verbose)
            show_summary(graph)
            print("\n[OK] Loaded all Safety AOI data")
        
        elif args.cleanup:
            cleanup_old_plcs(graph, args.verbose)
        
        elif args.summary:
            show_summary(graph)
        
        else:
            parser.print_help()
    
    finally:
        graph.close()


if __name__ == "__main__":
    main()
