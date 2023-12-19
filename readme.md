## Data Tracking Import
A bunch of import scripts to read data from SIL API's into our DB

## Usage
These scripts are wrapped into a Docker container, so it's easiest to just use that
### Prepare .env file
Use the following settings, replace `<placeholders>` with actual values
```text
# App settings
STAGE=prod

# Progress Bible
PB_BASE_URL=<url>
PB_KEY=<key>
PB_SECRET=<secret>

# Joshua Project
JP_BASE_URL=<unknown>

# TrackingDB
TDB_HOST=<mysql_host_name>
TDB_USER=<username>
TDB_PASSWORD=<password>
TDB_DB=<database_name>
```

### Pull
```commandline
docker pull unfoldingword/data-tracking-import
```

### Run
```commandline
docker run --rm --env-file .env -it unfoldingword/data-tracking-import python progress_bible.py
```

## Development
First, clone this repo. Then, inside the repo directory:

### Create and activate a virtualenv
Setting up a virtual environment keeps all the requirements and libraries for this project in one place.
```commandline
python3 -m ./venv
source ./venv/bin/activate
```

### Install requirements
```commandline
pip install -r requirements.txt
```

### Setup your .env file
See above

### Run one of the scripts
```
python3 ./progress_bible.py
```

When you're done, you can deactivate your virtual environment
```commandline
deactivate
```