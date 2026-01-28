#!/usr/bin/env python3
"""
Sample Data Loader for Pharma MES Ontology.

Loads sample Axilumab mAb production data to demonstrate the MES ontology extension.

Usage:
    python load_pharma_sample.py --load-all
    python load_pharma_sample.py --load-materials
    python load_pharma_sample.py --link-equipment  # Link Equipment to existing AOIs
"""

from neo4j_ontology import get_ontology_graph
from mes_ontology import extend_ontology


def load_sample_data(graph, verbose: bool = False):
    """Load all sample pharma data."""
    
    # Create schema first
    graph.create_mes_schema()
    if verbose:
        print("[OK] Created MES schema")
    
    # Load in dependency order
    load_materials(graph, verbose)
    load_vendors(graph, verbose)
    load_functional_locations(graph, verbose)
    load_equipment(graph, verbose)
    load_ccps(graph, verbose)
    load_batches(graph, verbose)
    load_production_orders(graph, verbose)


def load_materials(graph, verbose: bool = False):
    """Load material master data."""
    materials = [
        # Finished product
        {"matnr": "AXIL-DP-001", "description": "Axilumab Drug Product 150mg/mL", 
         "material_type": "FERT", "base_unit": "VL"},
        
        # Drug substance (intermediate)
        {"matnr": "AXIL-DS-001", "description": "Axilumab Drug Substance Bulk",
         "material_type": "HALB", "base_unit": "L"},
        
        # Harvest intermediate
        {"matnr": "AXIL-HV-001", "description": "Axilumab Harvest Intermediate",
         "material_type": "HALB", "base_unit": "L"},
        
        # Raw materials
        {"matnr": "RM-CHO-001", "description": "CHO Cell Bank (WCB)",
         "material_type": "ROH", "base_unit": "VL"},
        {"matnr": "RM-MED-001", "description": "Cell Culture Medium CD CHO",
         "material_type": "ROH", "base_unit": "L"},
        {"matnr": "RM-GLU-001", "description": "Glucose Feed Solution 400g/L",
         "material_type": "ROH", "base_unit": "L"},
        {"matnr": "RM-PAR-001", "description": "Protein A Resin MabSelect SuRe",
         "material_type": "ROH", "base_unit": "L"},
        {"matnr": "RM-BUF-001", "description": "Formulation Buffer",
         "material_type": "ROH", "base_unit": "L"},
    ]
    
    for mat in materials:
        graph.create_material(**mat)
    
    if verbose:
        print(f"[OK] Loaded {len(materials)} materials")


def load_vendors(graph, verbose: bool = False):
    """Load vendor data."""
    vendors = [
        {"lifnr": "V-001", "name": "Cytiva Life Sciences", 
         "vendor_type": "Equipment", "qualified": True},
        {"lifnr": "V-002", "name": "Sartorius AG",
         "vendor_type": "Equipment", "qualified": True},
        {"lifnr": "V-003", "name": "Thermo Fisher Scientific",
         "vendor_type": "Equipment", "qualified": True},
        {"lifnr": "V-004", "name": "Repligen Corporation",
         "vendor_type": "Consumables", "qualified": True},
    ]
    
    for v in vendors:
        graph.create_vendor(**v)
    
    if verbose:
        print(f"[OK] Loaded {len(vendors)} vendors")


def load_functional_locations(graph, verbose: bool = False):
    """Load functional locations (plant hierarchy)."""
    locations = [
        {"tplnr": "PLANT-001", "description": "Biopharma Manufacturing Site",
         "classification": "Plant"},
        {"tplnr": "PLANT-001-USP", "description": "Upstream Processing Suite",
         "classification": "Production Area", "gmp_classification": "Grade C"},
        {"tplnr": "PLANT-001-DSP", "description": "Downstream Processing Suite",
         "classification": "Production Area", "gmp_classification": "Grade C"},
        {"tplnr": "PLANT-001-FILL", "description": "Fill/Finish Suite",
         "classification": "Production Area", "gmp_classification": "Grade A/B"},
    ]
    
    for loc in locations:
        graph.create_functional_location(**loc)
    
    if verbose:
        print(f"[OK] Loaded {len(locations)} functional locations")


def load_equipment(graph, verbose: bool = False):
    """Load equipment and link to functional locations."""
    equipment_list = [
        # Bioreactors
        {"name": "BR-500-001", "equipment_type": "Bioreactor 500L",
         "location": "PLANT-001-USP", "validation_status": "Qualified",
         "plc_tag_prefix": "BR500_01"},
        {"name": "BR-2000-001", "equipment_type": "Bioreactor 2000L",
         "location": "PLANT-001-USP", "validation_status": "Qualified",
         "plc_tag_prefix": "BR2000_01"},
        
        # Chromatography
        {"name": "CHR-PA-001", "equipment_type": "Protein A Chromatography Skid",
         "location": "PLANT-001-DSP", "validation_status": "Qualified",
         "plc_tag_prefix": "CHRPA_01"},
        {"name": "CHR-IEX-001", "equipment_type": "Ion Exchange Chromatography Skid",
         "location": "PLANT-001-DSP", "validation_status": "Qualified",
         "plc_tag_prefix": "CHRIEX_01"},
        
        # Viral inactivation
        {"name": "VI-001", "equipment_type": "Viral Inactivation Vessel",
         "location": "PLANT-001-DSP", "validation_status": "Qualified",
         "plc_tag_prefix": "VI_01"},
        
        # Filtration
        {"name": "UF-001", "equipment_type": "Ultrafiltration/Diafiltration Skid",
         "location": "PLANT-001-DSP", "validation_status": "Qualified",
         "plc_tag_prefix": "UF_01"},
        {"name": "VF-001", "equipment_type": "Viral Filtration Skid",
         "location": "PLANT-001-DSP", "validation_status": "Qualified",
         "plc_tag_prefix": "VF_01"},
        
        # Fill/Finish
        {"name": "FILL-001", "equipment_type": "Vial Filling Line",
         "location": "PLANT-001-FILL", "validation_status": "Qualified",
         "plc_tag_prefix": "FILL_01"},
        {"name": "LYO-001", "equipment_type": "Lyophilizer",
         "location": "PLANT-001-FILL", "validation_status": "Qualified",
         "plc_tag_prefix": "LYO_01"},
    ]
    
    with graph.session() as session:
        for eq in equipment_list:
            # Create equipment
            session.run("""
                MERGE (e:Equipment {name: $name})
                SET e.equipment_type = $equipment_type,
                    e.validation_status = $validation_status,
                    e.plc_tag_prefix = $plc_tag_prefix
            """, eq)
            
            # Link to location
            session.run("""
                MATCH (e:Equipment {name: $name})
                MATCH (f:FunctionalLocation {tplnr: $location})
                MERGE (e)-[:LOCATED_IN]->(f)
            """, {"name": eq["name"], "location": eq["location"]})
    
    if verbose:
        print(f"[OK] Loaded {len(equipment_list)} equipment")


def load_ccps(graph, verbose: bool = False):
    """Load Critical Control Points and link to equipment."""
    ccps = [
        # Bioreactor CCPs
        {"ccp_id": "CCP-BR-TEMP", "parameter_name": "Temperature",
         "target": 37.0, "low_limit": 36.0, "high_limit": 38.0,
         "criticality": "Critical", "equipment": "BR-500-001"},
        {"ccp_id": "CCP-BR-PH", "parameter_name": "pH",
         "target": 7.0, "low_limit": 6.8, "high_limit": 7.2,
         "criticality": "Critical", "equipment": "BR-500-001"},
        {"ccp_id": "CCP-BR-DO", "parameter_name": "Dissolved Oxygen",
         "target": 40.0, "low_limit": 30.0, "high_limit": 60.0,
         "criticality": "Major", "equipment": "BR-500-001"},
        
        # Viral Inactivation CCPs
        {"ccp_id": "CCP-VI-PH", "parameter_name": "Low pH Hold",
         "target": 3.6, "low_limit": 3.4, "high_limit": 3.8,
         "criticality": "Critical", "equipment": "VI-001"},
        {"ccp_id": "CCP-VI-TIME", "parameter_name": "Hold Time",
         "target": 60.0, "low_limit": 60.0, "high_limit": 120.0,
         "criticality": "Critical", "equipment": "VI-001",
         "unit": "minutes"},
        
        # Filtration CCPs
        {"ccp_id": "CCP-VF-INTEG", "parameter_name": "Filter Integrity",
         "target": 3000.0, "low_limit": 2800.0, "high_limit": 9999.0,
         "criticality": "Critical", "equipment": "VF-001",
         "unit": "mbar"},
        
        # Fill CCPs
        {"ccp_id": "CCP-FILL-WT", "parameter_name": "Fill Weight",
         "target": 1.0, "low_limit": 0.95, "high_limit": 1.05,
         "criticality": "Critical", "equipment": "FILL-001",
         "unit": "mL"},
    ]
    
    for ccp in ccps:
        equipment = ccp.pop("equipment", None)
        graph.create_ccp(**ccp, equipment_name=equipment)
    
    if verbose:
        print(f"[OK] Loaded {len(ccps)} Critical Control Points")


def load_batches(graph, verbose: bool = False):
    """Load batch data."""
    batches = [
        {"charg": "HCC2601001", "matnr": "AXIL-HV-001", "quantity": 450.0,
         "status": "Released", "manufactured_date": "2026-01-15"},
        {"charg": "DS2601001", "matnr": "AXIL-DS-001", "quantity": 12.5,
         "status": "In Process", "manufactured_date": "2026-01-20"},
        {"charg": "DP2601001", "matnr": "AXIL-DP-001", "quantity": 5000.0,
         "status": "Pending QC", "manufactured_date": "2026-01-25",
         "quantity_unit": "vials"},
    ]
    
    for batch in batches:
        graph.create_batch(**batch)
    
    if verbose:
        print(f"[OK] Loaded {len(batches)} batches")


def load_production_orders(graph, verbose: bool = False):
    """Load production orders and operations."""
    orders = [
        {
            "aufnr": "PO-2601-001", "matnr": "AXIL-HV-001", "batch": "HCC2601001",
            "target_quantity": 500.0, "status": "TECO",
            "operations": [
                {"vornr": "0010", "description": "Seed Train Expansion", "equipment": "BR-500-001"},
                {"vornr": "0020", "description": "Production Culture", "equipment": "BR-2000-001"},
                {"vornr": "0030", "description": "Harvest Clarification", "equipment": None},
            ]
        },
        {
            "aufnr": "PO-2601-002", "matnr": "AXIL-DS-001", "batch": "DS2601001",
            "target_quantity": 15.0, "status": "REL",
            "operations": [
                {"vornr": "0010", "description": "Protein A Capture", "equipment": "CHR-PA-001"},
                {"vornr": "0020", "description": "Viral Inactivation", "equipment": "VI-001"},
                {"vornr": "0030", "description": "IEX Polishing", "equipment": "CHR-IEX-001"},
                {"vornr": "0040", "description": "Viral Filtration", "equipment": "VF-001"},
                {"vornr": "0050", "description": "UF/DF Concentration", "equipment": "UF-001"},
            ]
        },
        {
            "aufnr": "PO-2601-003", "matnr": "AXIL-DP-001", "batch": "DP2601001",
            "target_quantity": 6000.0, "status": "REL",
            "operations": [
                {"vornr": "0010", "description": "Formulation", "equipment": None},
                {"vornr": "0020", "description": "Sterile Filtration", "equipment": None},
                {"vornr": "0030", "description": "Vial Filling", "equipment": "FILL-001"},
                {"vornr": "0040", "description": "Visual Inspection", "equipment": None},
            ]
        },
    ]
    
    for order in orders:
        operations = order.pop("operations", [])
        graph.create_production_order(**order)
        
        for op in operations:
            graph.create_operation(order["aufnr"], **op)
    
    if verbose:
        print(f"[OK] Loaded {len(orders)} production orders")


def link_equipment_to_aois(graph, verbose: bool = False):
    """
    Link Equipment nodes to existing AOI nodes.
    
    This is the critical integration step - it connects the MES layer
    to Leor's existing PLC/SCADA layer.
    
    Call this AFTER both layers are loaded.
    """
    # These mappings depend on what AOIs exist in the database
    # The format is: equipment_name -> aoi_name
    
    # First, find what AOIs exist
    with graph.session() as session:
        result = session.run("MATCH (a:AOI) RETURN a.name as name")
        existing_aois = {r["name"] for r in result}
    
    if verbose:
        print(f"[INFO] Found {len(existing_aois)} existing AOIs")
    
    # Define potential mappings (adjust based on actual AOI names)
    potential_mappings = {
        "BR-500-001": ["Bioreactor_Control", "BR_Control", "Bioreactor"],
        "BR-2000-001": ["Bioreactor_Control", "BR_Control", "Bioreactor"],
        "CHR-PA-001": ["Chromatography_Control", "CHR_Control", "ChromSkid"],
        "CHR-IEX-001": ["Chromatography_Control", "CHR_Control", "ChromSkid"],
        "VI-001": ["VIVessel_Control", "VI_Control", "ViralInactivation"],
        "UF-001": ["UFDF_Control", "UF_Control", "Filtration"],
        "VF-001": ["VF_Control", "ViralFilter", "Filtration"],
        "FILL-001": ["Filler_Control", "Fill_Control", "FillingLine"],
        "LYO-001": ["Lyo_Control", "Lyophilizer_Control"],
    }
    
    linked = 0
    for equipment, aoi_candidates in potential_mappings.items():
        for aoi in aoi_candidates:
            if aoi in existing_aois:
                if graph.link_equipment_to_aoi(equipment, aoi):
                    if verbose:
                        print(f"  Linked {equipment} -> {aoi}")
                    linked += 1
                break
    
    if verbose:
        print(f"[OK] Created {linked} Equipment-AOI links")
    
    return linked


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Load Pharma MES Sample Data")
    parser.add_argument('--load-all', action='store_true',
                       help='Load all sample data')
    parser.add_argument('--load-materials', action='store_true',
                       help='Load materials only')
    parser.add_argument('--load-equipment', action='store_true',
                       help='Load equipment only')
    parser.add_argument('--load-ccps', action='store_true',
                       help='Load CCPs only')
    parser.add_argument('--load-batches', action='store_true',
                       help='Load batches and orders only')
    parser.add_argument('--link-equipment', action='store_true',
                       help='Link Equipment to existing AOIs')
    parser.add_argument('--create-schema', action='store_true',
                       help='Create MES schema only')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    graph = get_ontology_graph()
    extend_ontology(graph)
    
    try:
        if args.load_all:
            load_sample_data(graph, args.verbose)
            link_equipment_to_aois(graph, args.verbose)
            print("[OK] Loaded all sample data")
        
        elif args.create_schema:
            graph.create_mes_schema()
            print("[OK] Created MES schema")
        
        elif args.load_materials:
            load_materials(graph, args.verbose)
        
        elif args.load_equipment:
            load_functional_locations(graph, args.verbose)
            load_equipment(graph, args.verbose)
        
        elif args.load_ccps:
            load_ccps(graph, args.verbose)
        
        elif args.load_batches:
            load_batches(graph, args.verbose)
            load_production_orders(graph, args.verbose)
        
        elif args.link_equipment:
            link_equipment_to_aois(graph, args.verbose)
        
        else:
            parser.print_help()
    
    finally:
        graph.close()


if __name__ == "__main__":
    main()
