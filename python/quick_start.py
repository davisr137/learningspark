import os
import config

def read_readme_file(spark):
    filename = os.path.join(config.HOME, "README.md")
    text_file = spark.read.text(filename)
    return text_file
