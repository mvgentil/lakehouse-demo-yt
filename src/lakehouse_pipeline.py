from lakehouse_01_bronze import load_bronze
from lakehouse_02_silver import load_silver
from lakehouse_03_gold import load_gold

if __name__ == "__main__":
    load_bronze()
    load_silver()
    load_gold()