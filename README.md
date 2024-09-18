# About

This is a compilation of my data pipeline scripts written in Python.

### Conventions

Each pipeline function is an executable python file that accepts flags to modify the specific configurations of the pipeline (i.e. MSSQL DB Name, GCS Bucket Name).

When loading data from a third-party source you can set the temporary destination of the data to the `data/` folder. After the data has been successfully ingested remove the file from the data folder.

### Folder Structure

- config/
    - contains any specific configurations that need to be modified within the Docker container
-  data/
    - a temporary landing zone for any data that is ingested from a third-party source
- functions/
    - contains all pipeline functions
- functions/utils/
    - contains all reusable code and can be organized futher as either a Source (where data is pulled from), or a Target (where data is placed)

# Setup

[//]: # (TODO: Review environment and dependency management best practices when using Hatch)

1. Create and activate a python virtual environment.

```
python3 -m venv venv
```

```
source /venv/bin/activate
```


2. Install python dependencies

```
pip install -r requirements.txt
```