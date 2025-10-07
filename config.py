"""
ClearVue Configuration Manager
Handles MongoDB connection for both Atlas (cloud) and Local Docker

Author: ClearVue Streaming Team
Date: October 2025
"""

import os

class ClearVueConfig:
    """Configuration for MongoDB and Kafka connections"""
    
    # MongoDB Atlas Configuration (Cloud)
    MONGODB_ATLAS_URI = 'mongodb+srv://Tyra:1234@novacluster.1re1a4e.mongodb.net/?authSource=admin&retryWrites=true&w=majority'
    MONGODB_ATLAS_DB = 'Nova_Analytics'
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
    
    # Default: Use Atlas (change to 'local' for Docker MongoDB)
    MONGODB_MODE = os.environ.get('MONGODB_MODE', 'atlas')  # 'atlas' or 'local'
    
    @classmethod
    def get_mongo_uri(cls):
        """Get MongoDB URI based on mode"""
        if cls.MONGODB_MODE == 'local':
            return cls.MONGODB_LOCAL_URI
        return cls.MONGODB_ATLAS_URI
    
    @classmethod
    def get_database_name(cls):
        """Get database name based on mode"""
        if cls.MONGODB_MODE == 'local':
            return cls.MONGODB_LOCAL_DB
        return cls.MONGODB_ATLAS_DB
    
    @classmethod
    def get_kafka_servers(cls):
        """Get Kafka bootstrap servers"""
        return cls.KAFKA_BOOTSTRAP_SERVERS
    
    @classmethod
    def print_config(cls):
        """Print current configuration"""
        print("\n" + "="*70)
        print("CLEARVUE CONFIGURATION")
        print("="*70)
        print(f"MongoDB Mode: {cls.MONGODB_MODE.upper()}")
        
        if cls.MONGODB_MODE == 'local':
            print(f"MongoDB URI: mongodb://localhost:27017")
            print(f"Database: {cls.MONGODB_LOCAL_DB}")
            print("⚠️  Using LOCAL Docker MongoDB")
        else:
            print(f"MongoDB URI: mongodb+srv://...@novacluster.1re1a4e.mongodb.net")
            print(f"Database: {cls.MONGODB_ATLAS_DB}")
            print("☁️  Using MongoDB ATLAS (Cloud)")
        
        print(f"Kafka: {', '.join(cls.KAFKA_BOOTSTRAP_SERVERS)}")
        print("="*70 + "\n")
    
    @classmethod
    def set_mode(cls, mode):
        """Set MongoDB mode"""
        if mode not in ['atlas', 'local']:
            raise ValueError("Mode must be 'atlas' or 'local'")
        cls.MONGODB_MODE = mode
        os.environ['MONGODB_MODE'] = mode


# Quick access functions
def get_mongo_connection():
    """Get MongoDB connection string and database name"""
    return ClearVueConfig.get_mongo_uri(), ClearVueConfig.get_database_name()


def use_local_mongodb():
    """Switch to local Docker MongoDB"""
    ClearVueConfig.set_mode('local')


def use_atlas_mongodb():
    """Switch to Atlas cloud MongoDB"""
    ClearVueConfig.set_mode('atlas')