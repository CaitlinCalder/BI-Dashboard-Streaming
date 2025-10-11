"""
ClearVue Configuration Manager - Complete Version
Handles MongoDB Atlas connection and Kafka configuration

Author: ClearVue Streaming Team
Date: October 2025
"""

import os


class ClearVueConfig:
    """Configuration for MongoDB Atlas and Kafka connections"""
    
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
    
    # =====================================================================
    # MONITORING CONFIGURATION
    # =====================================================================
    STATS_PRINT_INTERVAL = 60  # Print stats every 60 seconds
    HEALTHCHECK_INTERVAL = 30  # Health check every 30 seconds
    
    # =====================================================================
    # CLASS METHODS
    # =====================================================================
    
    @classmethod
    def get_mongo_uri(cls):
        """Get MongoDB Atlas URI"""
        return cls.MONGODB_ATLAS_URI
    
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