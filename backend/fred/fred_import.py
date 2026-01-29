from pathlib import Path
import yaml







class fred_import:
    class_attribute = "insert static elements"

    def __init__(self):
        BASE_DIR = Path(__file__).resolve().parents[2]
        CONFIG_PATH = BASE_DIR / "config.yml"

        #C:\Users\jacob\OneDrive\Desktop\Projects\macro_dashboard\config.yml

        with open(CONFIG_PATH, 'r') as file:
            
            config = yaml.safe_load(file)

        try

        fred_api_key = config["api_keys"]["fred"]

