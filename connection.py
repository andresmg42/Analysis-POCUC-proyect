import yaml
import os
import sys
from sqlalchemy import create_engine


def connect():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

    config_file_path = os.path.join(script_dir, "config_fill.yml")

    with open(config_file_path, "r") as f:
        config = yaml.safe_load(f)
        config_co = config["POCUCDB"]
        
        

    # Construct the database URL
    url_co = (
        f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
        f"{config_co['port']}/{config_co['dbname']}"
    )
    
    # Create the SQLAlchemy Engine
    co_oltp = create_engine(url_co)
    
    return co_oltp