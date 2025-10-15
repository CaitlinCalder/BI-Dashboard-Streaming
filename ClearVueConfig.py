"""
ClearVue Configuration Manager - Complete Version
Handles MongoDB Atlas connection and Kafka configuration

Author: ClearVue Streaming Team
Date: October 2025
"""

import os
import logging

class ClearVueConfig:
    """Configuration for MongoDB Atlas and Kafka connections"""
    POWERBI_WORKSPACE_ID = 'ee71b5e8-e84c-4b9c-9289-c738651a2c05'
    POWERBI_DATASET_ID = 'f15b2b99-8dc2-4367-980a-6d0a365874e4'
    POWERBI_ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyIsImtpZCI6IkhTMjNiN0RvN1RjYVUxUm9MSHdwSXEyNFZZZyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvYjE0ZDg2ZjEtODNiYS00YjEzLWE3MDItYjVjMDIzMWI5MzM3LyIsImlhdCI6MTc2MDUxNzY0MCwibmJmIjoxNzYwNTE3NjQwLCJleHAiOjE3NjA1MjIzNTQsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVVFBdS84YUFBQUFaU3FKUFFaWVhxYXM4Nm5rVnFmVTBFZHp1ZXFNODQ0OUdub0FoR2syQ3F2WU5sTlFTRVozRUZ4bUUrVUxXTFNQcllyZmRNQU96c3dKRHVRRlpyaytpUT09IiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiQ2FsZGVyIiwiZ2l2ZW5fbmFtZSI6IkMiLCJpZHR5cCI6InVzZXIiLCJpbl9jb3JwIjoidHJ1ZSIsImlwYWRkciI6IjE5Ny4xODUuMTc5LjEyMiIsIm5hbWUiOiJDIENhbGRlciIsIm9pZCI6Ijc2MThhNjNjLTZhMjktNDNiNC05N2RkLTk3YmYxZDkyOTZjZiIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0xMjk5MTc1MzAzLTE3NjcxNDUyODItMzUwMzMwNTAyMy03MzE1NCIsInB1aWQiOiIxMDAzMjAwMjZFRjUxMDBGIiwicmgiOiIxLkFZRUE4WVpOc2JxREUwdW5BclhBSXh1VE53a0FBQUFBQUFBQXdBQUFBQUFBQUFDQkFPNkJBQS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzaWQiOiIwMDlhYzE0OS1jNWE1LTgyMDQtNzEzZi0wNDZmYjNkZGUzZWUiLCJzdWIiOiJtNkpiZUo2TklUYkhzTEhja09KVl91N25QdHJaVXg2Zko5MHJ1MmNZSmswIiwidGlkIjoiYjE0ZDg2ZjEtODNiYS00YjEzLWE3MDItYjVjMDIzMWI5MzM3IiwidW5pcXVlX25hbWUiOiI0NjE4NDAwN0BteW53dS5hYy56YSIsInVwbiI6IjQ2MTg0MDA3QG15bnd1LmFjLnphIiwidXRpIjoiOGFmNm5oVFV4azJFZ1ZnamNOc0JBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19jYyI6WyJDUDEiXSwieG1zX2Z0ZCI6IkgyOWQ1MnViYWM5TUZlSlNzXzV5U1l3SUd1Z0pXVV8tdG42QzNXLTZCNDhCWlhWeWIzQmxibTl5ZEdndFpITnRjdyIsInhtc19pZHJlbCI6IjEgNiJ9.XimKvf3xLU_bCoVHRjB47fRUFoeOhp_ZIxf-eNDLPyuSXOOFLzNR_mgV50gVFkl_Ds3XcjYRwLjaPglZYFDSaAx8jlSd1_5xw7-UdjxiN9yqid4ogaT46lh89XaGYnOg9TEr-K6prXbLs8yLThUnVSUXRK9cHlPucN-zT9lonreQ5sQ4Ow302l42aJzgLSIbgSGm76C1zs2aS2ayklhdvojJgDVGvY7tJ5TJbTlvMV3Bz-E-tjeUfg9sqXXP_weCNJRPPK9fbHcaBV-aNHT4jjiUa2BeaVR0h2g5awoyRzEl-NE1OBmni6won1x_wXe0g7BaQJ6pug5IVRIG6Vl8Cw'


    # Refresh behavior
    POWERBI_MIN_REFRESH_INTERVAL = 60   # Min 60 seconds between refreshes
    POWERBI_BATCH_WINDOW = 30            # Wait 30 seconds to batch changes

    @classmethod
    def get_powerbi_workspace_id(cls):
        return cls.POWERBI_WORKSPACE_ID

    @classmethod
    def get_powerbi_dataset_id(cls):
        return cls.POWERBI_DATASET_ID

    @classmethod
    def get_powerbi_access_token(cls):
        return cls.POWERBI_ACCESS_TOKEN
    # =====================================================================
    # MONGODB ATLAS CONFIGURATION
    # =====================================================================
    MONGODB_ATLAS_URI = 'mongodb+srv://christischoeman_db_user:1234@novacluster.1re1a4e.mongodb.net/?authSource=admin&retryWrites=true&w=majority'
    MONGODB_ATLAS_DB = 'Nova_Analytix'
    
    # Collection names (exact match with Atlas)
    COLLECTIONS = {
        'sales': 'Sales_flat',
        'payments': 'Payments_flat',
        'purchases': 'Purchases_flat',
        'customers': 'Customer_flat_step2',
        'products': 'Products_flat',
        'suppliers': 'Suppliers'
    }
    
    # =====================================================================
    # KAFKA CONFIGURATION
    # =====================================================================
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
    
    # Kafka topics configuration
    KAFKA_TOPICS = {
        'Sales_flat': {
            'topic': 'clearvue.sales',
            'partitions': 3,
            'priority': 'HIGH'
        },
        'Payments_flat': {
            'topic': 'clearvue.payments',
            'partitions': 3,
            'priority': 'HIGH'
        },
        'Purchases_flat': {
            'topic': 'clearvue.purchases',
            'partitions': 2,
            'priority': 'MEDIUM'
        },
        'Customer_flat_step2': {
            'topic': 'clearvue.customers',
            'partitions': 2,
            'priority': 'MEDIUM'
        },
        'Products_flat': {
            'topic': 'clearvue.products',
            'partitions': 2,
            'priority': 'MEDIUM'
        },
        'Suppliers': {
            'topic': 'clearvue.suppliers',
            'partitions': 1,
            'priority': 'LOW'
        }
    }
    
    # Kafka producer configuration
    KAFKA_PRODUCER_CONFIG = {
        'acks': 'all',
        'retries': 3,
        'max_in_flight_requests_per_connection': 5,
        'compression_type': 'snappy',
        'linger_ms': 10,
        'batch_size': 16384
    }
    
    # =====================================================================
    # MONGODB CHANGE STREAM CONFIGURATION
    # =====================================================================
    CHANGE_STREAM_CONFIG = {
        'full_document': 'updateLookup',
        'max_await_time_ms': 1000
    }
    POWERBI_PUSH_URL = 'https://api.powerbi.com/beta/b14d86f1-83ba-4b13-a702-b5c0231b9337/datasets/7bd18a13-0815-4cbf-bb41-84ab2a2383bc/rows?experience=power-bi&key=dUOGa9OQ%2B%2BgifLK%2F0%2BEzT9xJuvJ18UZrrz5Wfrf6WWamfa%2Fy20uYZB2buv6TCK9T9PWaWAVc3t%2FX%2B5TC6QtyLg%3D%3D'
    

    # =====================================================================
    # MONITORING CONFIGURATION
    # =====================================================================
    STATS_PRINT_INTERVAL = 60  # Print stats every 60 seconds
    HEALTHCHECK_INTERVAL = 30  # Health check every 30 seconds
    
    # =====================================================================
    # CLASS METHODS
    # =====================================================================
    @classmethod
    def validate_powerbi_config(cls) -> bool:
        """Validate Power BI configuration"""
        url = cls.get_powerbi_push_url()
        
        if not url:
            return False
        
        if not url.startswith('https://api.powerbi.com/'):
            return False
        
        if '/datasets/' not in url or '/rows?' not in url:
            return False
        
        if 'key=' not in url:
            return False
        
        return True

    @classmethod
    def get_mongo_uri(cls):
        """Get MongoDB Atlas URI"""
        return cls.MONGODB_ATLAS_URI
    
    @staticmethod
    def get_powerbi_push_url():
        """Get Power BI streaming dataset push URL"""
        return ClearVueConfig.POWERBI_PUSH_URL

    @classmethod
    def get_database_name(cls):
        """Get database name"""
        return cls.MONGODB_ATLAS_DB
    
    @classmethod
    def get_collection_name(cls, alias: str) -> str:
        """
        Get actual collection name from alias
        
        Args:
            alias: Friendly name (e.g., 'sales')
        
        Returns:
            Actual collection name (e.g., 'Sales_flat')
        """
        return cls.COLLECTIONS.get(alias, alias)
    
    @classmethod
    def get_all_collections(cls) -> list:
        """Get list of all collection names"""
        return list(cls.COLLECTIONS.values())
    
    @classmethod
    def get_kafka_servers(cls):
        """Get Kafka bootstrap servers"""
        return cls.KAFKA_BOOTSTRAP_SERVERS
    
    @classmethod
    def get_topic_for_collection(cls, collection_name: str) -> str:
        """
        Get Kafka topic name for a collection
        
        Args:
            collection_name: MongoDB collection name
        
        Returns:
            Kafka topic name
        """
        config = cls.KAFKA_TOPICS.get(collection_name, {})
        return config.get('topic', f'clearvue.{collection_name.lower()}')
    
    @classmethod
    def get_collection_priority(cls, collection_name: str) -> str:
        """
        Get priority level for a collection
        
        Args:
            collection_name: MongoDB collection name
        
        Returns:
            Priority level: 'HIGH', 'MEDIUM', or 'LOW'
        """
        config = cls.KAFKA_TOPICS.get(collection_name, {})
        return config.get('priority', 'MEDIUM')
    
    @classmethod
    def print_config(cls, detailed: bool = False):
        """
        Print current configuration
        
        Args:
            detailed: If True, print detailed configuration
        """
        print("\n" + "="*70)
        print("CLEARVUE CONFIGURATION")
        print("="*70)
        print("  MongoDB Atlas (Cloud)")
        print(f"   URI: mongodb+srv://...@novacluster.1re1a4e.mongodb.net")
        print(f"   Database: {cls.MONGODB_ATLAS_DB}")
        print(f"   Collections: {len(cls.COLLECTIONS)}")
        
        if detailed:
            print("\n📊 Collections:")
            for alias, actual in cls.COLLECTIONS.items():
                priority = cls.get_collection_priority(actual)
                topic = cls.get_topic_for_collection(actual)
                print(f"   {actual:22} → {topic:25} ({priority})")
        
        print(f"\n Kafka: {', '.join(cls.KAFKA_BOOTSTRAP_SERVERS)}")
        print(f"   Topics: {len(cls.KAFKA_TOPICS)}")
        print("="*70 + "\n")


# =====================================================================
# HELPER FUNCTIONS
# =====================================================================

def get_mongo_connection():
    """Get MongoDB connection string and database name"""
    return ClearVueConfig.get_mongo_uri(), ClearVueConfig.get_database_name()


def get_collections():
    """Get list of collections to access"""
    return ClearVueConfig.get_all_collections()


def get_connection_details():
    """Get full connection details as a dictionary"""
    return {
        'uri': ClearVueConfig.get_mongo_uri(),
        'database': ClearVueConfig.get_database_name(),
        'collections': ClearVueConfig.get_all_collections(),
        'kafka_servers': ClearVueConfig.get_kafka_servers()
    }
# Power BI Configuration
# Add this line

def print_startup_banner():
    """Print startup banner"""
    print("\n" + "="*70)
    print("   ______ __      ______   ___    ____    __  ________")
    print("  / ____// /     / ____/  /   |  / __ \\  | | / / ____/")
    print(" / /    / /     / __/    / /| | / /_/ /  | |/ / __/   ")
    print("/ /___ / /___  / /___   / ___ |/ _, _/   |   / /___   ")
    print("\\____//_____/ /_____/  /_/  |_/_/ |_|    |__/_____/   ")
    print("")
    print("           Real-time Streaming Analytics Platform")
    print("="*70)