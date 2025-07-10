#!/usr/bin/env python3
"""
Test script for multi-site insertion logic in ring network database updates.
This script demonstrates how the connection chain logic works for multiple insert sites.
"""

import pandas as pd

def test_connection_chain_logic():
    """Test the connection chain creation for multiple insert sites."""
    
    print("🔬 Testing Multi-Site Insertion Logic\n")
    
    # Example data: Ring with NE=A, FE=F, inserting sites B, C, D, E
    near_end = "Site_A"
    far_end = "Site_F"
    insert_sites = ["Site_B", "Site_C", "Site_D", "Site_E"]
    
    # Create the connection chain: NE → Site1 → Site2 → ... → SiteN → FE
    connection_chain = [near_end] + insert_sites + [far_end]
    print(f"🔗 Connection chain: {' → '.join(connection_chain)}")
    
    # Create connections for each link in the chain
    connections = []
    for i in range(len(connection_chain) - 1):
        origin_site = connection_chain[i]
        destination_site = connection_chain[i + 1]
        
        # Determine priorities
        if origin_site == near_end:
            priority_1 = 'Access'
        elif origin_site in insert_sites:
            priority_1 = 'Insert Site'
        else:
            priority_1 = 'Access'
            
        if destination_site == far_end:
            priority_2 = 'Access'
        elif destination_site in insert_sites:
            priority_2 = 'Insert Site'
        else:
            priority_2 = 'Access'
        
        connection = {
            'Ring ID': 'TEST_RING_001',
            'Origin Site ID': origin_site,
            'Destination': destination_site,
            'Priority_1': priority_1,
            'Priority_2': priority_2,
            'Link Name': f"{origin_site}-{destination_site}"
        }
        
        connections.append(connection)
        print(f"✅ Connection {i+1}: {origin_site} → {destination_site} | Priority: {priority_1} → {priority_2}")
    
    # Create DataFrame
    connections_df = pd.DataFrame(connections)
    print(f"\n📊 Generated {len(connections_df)} connections:")
    print(connections_df[['Origin Site ID', 'Destination', 'Priority_1', 'Priority_2', 'Link Name']].to_string(index=False))
    
    # Test different scenarios
    print(f"\n🧪 Testing Different Scenarios:")
    
    # Scenario 1: Single insert site
    test_single_insert()
    
    # Scenario 2: Two insert sites  
    test_two_insert_sites()
    
    # Scenario 3: Empty insert sites
    test_empty_insert_sites()

def test_single_insert():
    """Test with a single insert site."""
    print(f"\n📝 Scenario 1: Single Insert Site")
    
    near_end = "Site_A"
    far_end = "Site_C"
    insert_sites = ["Site_B"]
    
    connection_chain = [near_end] + insert_sites + [far_end]
    print(f"🔗 Chain: {' → '.join(connection_chain)}")
    
    expected_connections = [
        (near_end, insert_sites[0]),
        (insert_sites[0], far_end)
    ]
    
    for i, (origin, dest) in enumerate(expected_connections):
        print(f"  Connection {i+1}: {origin} → {dest}")

def test_two_insert_sites():
    """Test with two insert sites."""
    print(f"\n📝 Scenario 2: Two Insert Sites")
    
    near_end = "Site_A"
    far_end = "Site_D"
    insert_sites = ["Site_B", "Site_C"]
    
    connection_chain = [near_end] + insert_sites + [far_end]
    print(f"🔗 Chain: {' → '.join(connection_chain)}")
    
    expected_connections = [
        (near_end, insert_sites[0]),
        (insert_sites[0], insert_sites[1]),
        (insert_sites[1], far_end)
    ]
    
    for i, (origin, dest) in enumerate(expected_connections):
        print(f"  Connection {i+1}: {origin} → {dest}")

def test_empty_insert_sites():
    """Test with no insert sites."""
    print(f"\n📝 Scenario 3: No Insert Sites")
    
    near_end = "Site_A"
    far_end = "Site_B"
    insert_sites = []
    
    connection_chain = [near_end] + insert_sites + [far_end]
    print(f"🔗 Chain: {' → '.join(connection_chain)}")
    print(f"  This would result in direct connection: {near_end} → {far_end}")

def test_dataframe_partitioning():
    """Test DataFrame partitioning logic."""
    print(f"\n🔪 Testing DataFrame Partitioning Logic\n")
    
    # Create sample ring data
    ring_data = pd.DataFrame({
        'Ring ID': ['RING_001'] * 6,
        'Origin Site ID': ['Site_A', 'Site_B', 'Site_C', 'Site_D', 'Site_E', 'Site_F'],
        'Destination': ['Site_B', 'Site_C', 'Site_D', 'Site_E', 'Site_F', 'Site_A']
    })
    
    print("Original Ring Data:")
    print(ring_data.to_string(index=True))
    
    # Test partitioning with NE=Site_B and FE=Site_E
    ne_site_ids = ['Site_B']
    fe_site_ids = ['Site_E']
    
    start_location = ring_data[ring_data['Origin Site ID'].isin(ne_site_ids)]
    end_location = ring_data[ring_data['Destination'].isin(fe_site_ids)]
    
    if not start_location.empty and not end_location.empty:
        start_index = start_location.index[0]
        end_index = end_location.index[-1]
        
        start_position = ring_data.index.get_loc(start_index)
        end_position = ring_data.index.get_loc(end_index)
        
        print(f"\n🔍 Partitioning Analysis:")
        print(f"  NE Site: {ne_site_ids} found at index {start_index} (position {start_position})")
        print(f"  FE Site: {fe_site_ids} found at index {end_index} (position {end_position})")
        
        top_part = ring_data.iloc[:start_position + 1]
        bottom_part = ring_data.iloc[end_position:]
        
        print(f"\n📂 Top Part (before insertion point):")
        print(top_part.to_string(index=True))
        
        print(f"\n📂 Bottom Part (after insertion point):")
        print(bottom_part.to_string(index=True))
        
        # Simulate insertion
        insert_data = pd.DataFrame({
            'Ring ID': ['RING_001', 'RING_001'],
            'Origin Site ID': ['Site_B', 'Site_X'],
            'Destination': ['Site_X', 'Site_E']
        })
        
        print(f"\n📥 Insert Data:")
        print(insert_data.to_string(index=True))
        
        # Reconstruct
        new_ring = pd.concat([top_part, insert_data, bottom_part], ignore_index=True)
        
        print(f"\n🔄 Reconstructed Ring:")
        print(new_ring.to_string(index=True))

if __name__ == "__main__":
    test_connection_chain_logic()
    test_dataframe_partitioning()
    print(f"\n✅ All tests completed!")
