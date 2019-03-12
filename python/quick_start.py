import os
import config

def read_readme_file():
    filename = os.path.join(config.HOME, "README.md")
    text_file = config.spark.read.text(filename)
    return text_file
