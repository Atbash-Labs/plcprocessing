#!/usr/bin/env python3
"""
Sample Data Loader for ProveIT SIF/Safety MES Ontology.

Creates sample data that fits the existing ProveIT structure:
- Scripts for pss modules
- NamedQueries for business unit event journals
- Views for dashboards

This creates MES-level entities (Sites, SIFs, Demands) that link to the
existing Script/NQ infrastructure.

Usage:
    python load_proveit_sample.py --load-all -v
    python load_proveit_sample.py --link-scripts -v
"""

from neo4j_ontology import get_ontology_graph
from mes_ontology import extend_ontology


def load_sample_data(graph, verbose: bool = False):
    """Load all sample ProveIT MES data."""
    
    # Create schema first
    graph.create_mes_schema()
    if verbose:
        print("[OK] Created MES schema")
    
    # Load in dependency order
    create_proveit_schema(graph, verbose)
    load_business_units(graph, verbose)
    load_sites(graph, verbose)
    load_sifs(graph, verbose)
    load_demands(graph, verbose)
    link_to_scripts(graph, verbose)


def create_proveit_schema(graph, verbose: bool = False):
    """Create ProveIT-specific schema."""
    with graph.session() as session:
        constraints = [
            "CREATE CONSTRAINT bu_name IF NOT EXISTS FOR (b:BusinessUnit) REQUIRE b.name IS UNIQUE",
            "CREATE CONSTRAINT site_name IF NOT EXISTS FOR (s:Site) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT sif_id IF NOT EXISTS FOR (s:SIF) REQUIRE s.sif_id IS UNIQUE",
            "CREATE CONSTRAINT demand_id IF NOT EXISTS FOR (d:DemandEvent) REQUIRE d.demand_id IS UNIQUE",
        ]
        indexes = [
            "CREATE INDEX sif_site IF NOT EXISTS FOR (s:SIF) ON (s.site)",
            "CREATE INDEX demand_sif IF NOT EXISTS FOR (d:DemandEvent) ON (d.sif_id)",
            "CREATE INDEX demand_date IF NOT EXISTS FOR (d:DemandEvent) ON (d.demand_date)",
        ]
        for stmt in constraints + indexes:
            try:
                session.run(stmt)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"[WARNING] {e}")
    
    if verbose:
        print("[OK] Created ProveIT schema")


def load_business_units(graph, verbose: bool = False):
    """Load business units matching the existing NQ folders."""
    bus = [
        {"name": "PLX", "description": "PLX Business Unit", "region": "North America"},
        {"name": "MTR", "description": "MTR Business Unit", "region": "Europe"},
        {"name": "VXN", "description": "VXN Business Unit", "region": "Asia Pacific"},
        {"name": "CRD", "description": "CRD Business Unit", "region": "South America"},
        {"name": "NVL", "description": "NVL Business Unit", "region": "Middle East"},
    ]
    
    with graph.session() as session:
        for bu in bus:
            session.run("""
                MERGE (b:BusinessUnit {name: $name})
                SET b.description = $description,
                    b.region = $region
            """, bu)
            
            # Link BU to its NamedQueries
            session.run("""
                MATCH (b:BusinessUnit {name: $name})
                MATCH (q:NamedQuery)
                WHERE q.folder_path = $name
                MERGE (q)-[:BELONGS_TO_BU]->(b)
            """, {"name": bu["name"]})
    
    if verbose:
        print(f"[OK] Loaded {len(bus)} business units")


def load_sites(graph, verbose: bool = False):
    """Load sites (DataOwners) for each business unit."""
    sites = [
        # PLX sites
        {"name": "PLX-Site-Alpha", "bu": "PLX", "location": "Houston, TX", "timezone": "America/Chicago"},
        {"name": "PLX-Site-Beta", "bu": "PLX", "location": "Midland, TX", "timezone": "America/Chicago"},
        # MTR sites
        {"name": "MTR-Site-Nord", "bu": "MTR", "location": "Rotterdam, NL", "timezone": "Europe/Amsterdam"},
        {"name": "MTR-Site-Sud", "bu": "MTR", "location": "Marseille, FR", "timezone": "Europe/Paris"},
        # VXN sites
        {"name": "VXN-Site-East", "bu": "VXN", "location": "Singapore", "timezone": "Asia/Singapore"},
        {"name": "VXN-Site-West", "bu": "VXN", "location": "Mumbai, IN", "timezone": "Asia/Kolkata"},
        # CRD sites
        {"name": "CRD-Site-Rio", "bu": "CRD", "location": "Rio de Janeiro, BR", "timezone": "America/Sao_Paulo"},
        # NVL sites
        {"name": "NVL-Site-Gulf", "bu": "NVL", "location": "Abu Dhabi, UAE", "timezone": "Asia/Dubai"},
    ]
    
    with graph.session() as session:
        for site in sites:
            session.run("""
                MERGE (s:Site {name: $name})
                SET s.location = $location,
                    s.timezone = $timezone
            """, site)
            
            # Link to BU
            session.run("""
                MATCH (s:Site {name: $name})
                MATCH (b:BusinessUnit {name: $bu})
                MERGE (s)-[:PART_OF]->(b)
            """, {"name": site["name"], "bu": site["bu"]})
    
    if verbose:
        print(f"[OK] Loaded {len(sites)} sites")


def load_sifs(graph, verbose: bool = False):
    """Load Safety Instrumented Functions (SIFs)."""
    sifs = [
        # PLX SIFs
        {"sif_id": "PLX-SIF-001", "name": "High Pressure Trip", "site": "PLX-Site-Alpha",
         "sil_level": 2, "demand_mode": "Low", "proof_test_interval": 12},
        {"sif_id": "PLX-SIF-002", "name": "High Temperature Trip", "site": "PLX-Site-Alpha",
         "sil_level": 2, "demand_mode": "Low", "proof_test_interval": 12},
        {"sif_id": "PLX-SIF-003", "name": "Gas Detection ESD", "site": "PLX-Site-Beta",
         "sil_level": 3, "demand_mode": "Low", "proof_test_interval": 6},
        {"sif_id": "PLX-SIF-004", "name": "Fire Detection Deluge", "site": "PLX-Site-Beta",
         "sil_level": 2, "demand_mode": "Low", "proof_test_interval": 12},
        
        # MTR SIFs
        {"sif_id": "MTR-SIF-001", "name": "Reactor Overpressure", "site": "MTR-Site-Nord",
         "sil_level": 3, "demand_mode": "Low", "proof_test_interval": 6},
        {"sif_id": "MTR-SIF-002", "name": "Compressor Surge Protection", "site": "MTR-Site-Nord",
         "sil_level": 2, "demand_mode": "Low", "proof_test_interval": 12},
        {"sif_id": "MTR-SIF-003", "name": "Tank High Level", "site": "MTR-Site-Sud",
         "sil_level": 2, "demand_mode": "Low", "proof_test_interval": 12},
         
        # VXN SIFs
        {"sif_id": "VXN-SIF-001", "name": "Flare System ESD", "site": "VXN-Site-East",
         "sil_level": 3, "demand_mode": "Low", "proof_test_interval": 6},
        {"sif_id": "VXN-SIF-002", "name": "Burner Management", "site": "VXN-Site-West",
         "sil_level": 3, "demand_mode": "High", "proof_test_interval": 3},
         
        # CRD SIFs
        {"sif_id": "CRD-SIF-001", "name": "Emergency Blowdown", "site": "CRD-Site-Rio",
         "sil_level": 2, "demand_mode": "Low", "proof_test_interval": 12},
         
        # NVL SIFs
        {"sif_id": "NVL-SIF-001", "name": "HIPPS", "site": "NVL-Site-Gulf",
         "sil_level": 3, "demand_mode": "Low", "proof_test_interval": 6},
        {"sif_id": "NVL-SIF-002", "name": "Subsea Isolation", "site": "NVL-Site-Gulf",
         "sil_level": 3, "demand_mode": "Low", "proof_test_interval": 12},
    ]
    
    with graph.session() as session:
        for sif in sifs:
            session.run("""
                MERGE (s:SIF {sif_id: $sif_id})
                SET s.name = $name,
                    s.site = $site,
                    s.sil_level = $sil_level,
                    s.demand_mode = $demand_mode,
                    s.proof_test_interval_months = $proof_test_interval
            """, sif)
            
            # Link to Site
            session.run("""
                MATCH (sif:SIF {sif_id: $sif_id})
                MATCH (site:Site {name: $site})
                MERGE (sif)-[:LOCATED_AT]->(site)
            """, {"sif_id": sif["sif_id"], "site": sif["site"]})
    
    if verbose:
        print(f"[OK] Loaded {len(sifs)} SIFs")


def load_demands(graph, verbose: bool = False):
    """Load sample demand events."""
    demands = [
        # PLX demands
        {"demand_id": "DEM-PLX-2026-001", "sif_id": "PLX-SIF-001", "demand_date": "2026-01-15",
         "demand_type": "Real", "outcome": "Successful Trip", "description": "High pressure excursion during startup"},
        {"demand_id": "DEM-PLX-2026-002", "sif_id": "PLX-SIF-003", "demand_date": "2026-01-20",
         "demand_type": "Spurious", "outcome": "False Trip", "description": "Detector drift caused spurious activation"},
        
        # MTR demands
        {"demand_id": "DEM-MTR-2026-001", "sif_id": "MTR-SIF-001", "demand_date": "2026-01-10",
         "demand_type": "Real", "outcome": "Successful Trip", "description": "Reactor pressure exceeded setpoint"},
        
        # VXN demands
        {"demand_id": "DEM-VXN-2026-001", "sif_id": "VXN-SIF-002", "demand_date": "2026-01-22",
         "demand_type": "Proof Test", "outcome": "Pass", "description": "Quarterly proof test completed"},
         
        # NVL demands
        {"demand_id": "DEM-NVL-2026-001", "sif_id": "NVL-SIF-001", "demand_date": "2026-01-18",
         "demand_type": "Real", "outcome": "Successful Trip", "description": "HIPPS activated on downstream pressure loss"},
    ]
    
    with graph.session() as session:
        for dem in demands:
            session.run("""
                MERGE (d:DemandEvent {demand_id: $demand_id})
                SET d.sif_id = $sif_id,
                    d.demand_date = $demand_date,
                    d.demand_type = $demand_type,
                    d.outcome = $outcome,
                    d.description = $description
            """, dem)
            
            # Link to SIF
            session.run("""
                MATCH (d:DemandEvent {demand_id: $demand_id})
                MATCH (s:SIF {sif_id: $sif_id})
                MERGE (d)-[:DEMAND_ON]->(s)
            """, {"demand_id": dem["demand_id"], "sif_id": dem["sif_id"]})
    
    if verbose:
        print(f"[OK] Loaded {len(demands)} demand events")


def link_to_scripts(graph, verbose: bool = False):
    """Link MES entities to existing Scripts."""
    with graph.session() as session:
        # Link SIFs to the eventJournal script (handles demand logging)
        result = session.run("""
            MATCH (s:SIF)
            MATCH (script:Script)
            WHERE script.name CONTAINS 'eventJournal'
            MERGE (s)-[:LOGGED_BY]->(script)
            RETURN count(s) as cnt
        """)
        sif_links = result.single()["cnt"]
        
        # Link Sites to the dataOwners script
        result = session.run("""
            MATCH (site:Site)
            MATCH (script:Script)
            WHERE script.name CONTAINS 'dataOwners'
            MERGE (site)-[:MANAGED_BY]->(script)
            RETURN count(site) as cnt
        """)
        site_links = result.single()["cnt"]
        
        # Link BusinessUnits to their NQs (already done, verify)
        result = session.run("""
            MATCH (b:BusinessUnit)<-[:BELONGS_TO_BU]-(q:NamedQuery)
            RETURN b.name as bu, count(q) as nq_count
        """)
        bu_nqs = {r["bu"]: r["nq_count"] for r in result}
        
        if verbose:
            print(f"[OK] Linked {sif_links} SIFs to eventJournal scripts")
            print(f"[OK] Linked {site_links} Sites to dataOwners scripts")
            print(f"[OK] BU -> NQ links: {bu_nqs}")


def show_summary(graph, verbose: bool = False):
    """Show summary of loaded data."""
    with graph.session() as session:
        result = session.run("""
            MATCH (b:BusinessUnit)
            OPTIONAL MATCH (b)<-[:PART_OF]-(site:Site)
            OPTIONAL MATCH (site)<-[:LOCATED_AT]-(sif:SIF)
            OPTIONAL MATCH (sif)<-[:DEMAND_ON]-(dem:DemandEvent)
            RETURN b.name as bu,
                   count(DISTINCT site) as sites,
                   count(DISTINCT sif) as sifs,
                   count(DISTINCT dem) as demands
            ORDER BY b.name
        """)
        
        print("\n=== ProveIT MES Data Summary ===")
        print(f"{'BU':<8} {'Sites':<8} {'SIFs':<8} {'Demands':<8}")
        print("-" * 32)
        for r in result:
            print(f"{r['bu']:<8} {r['sites']:<8} {r['sifs']:<8} {r['demands']:<8}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Load ProveIT MES Sample Data")
    parser.add_argument('--load-all', action='store_true',
                       help='Load all sample data')
    parser.add_argument('--load-bus', action='store_true',
                       help='Load business units only')
    parser.add_argument('--load-sites', action='store_true',
                       help='Load sites only')
    parser.add_argument('--load-sifs', action='store_true',
                       help='Load SIFs only')
    parser.add_argument('--link-scripts', action='store_true',
                       help='Link entities to existing scripts')
    parser.add_argument('--summary', action='store_true',
                       help='Show data summary')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    graph = get_ontology_graph()
    extend_ontology(graph)
    
    try:
        if args.load_all:
            load_sample_data(graph, args.verbose)
            show_summary(graph, args.verbose)
            print("\n[OK] Loaded all sample data")
        
        elif args.load_bus:
            create_proveit_schema(graph, args.verbose)
            load_business_units(graph, args.verbose)
        
        elif args.load_sites:
            load_sites(graph, args.verbose)
        
        elif args.load_sifs:
            load_sifs(graph, args.verbose)
        
        elif args.link_scripts:
            link_to_scripts(graph, args.verbose)
        
        elif args.summary:
            show_summary(graph, args.verbose)
        
        else:
            parser.print_help()
    
    finally:
        graph.close()


if __name__ == "__main__":
    main()
