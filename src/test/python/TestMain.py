import pytest
from pyspark.sql import SparkSession
import sys
import os

from pyspark.sql.functions import udf

from conftest import sparksession
from pathlib import Path
from itertools import combinations
from itertools import permutations,combinations_with_replacement
import itertools
import io
from pyspark.sql.functions import *



from src.main.python import main



root_dir = Path.cwd().parent.parent
print(root_dir)
# root_dir = "\\".join(root_dir)
sys.path.append(str(root_dir)+"\\main\\python\\")
print(root_dir)
# print(TEST_DIR, PROJECT_DIR)
# sys.path.insert(0, PROJECT_DIR)



def test_main_function(sparksession):
    yaml_path = "yaml file path"
    # main.process_start(sparksession, yaml_path,"dev")
    df = sparksession.read.format("parquet").load("C:\\Users\\kabar\\Downloads\\userdata1.parquet")
