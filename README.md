# datagovmy-back

## Setup virtual environment

```bash
<your python exe> -m venv env

# activate and install dependencies
source env/bin/activate
pip install -r requirements.txt
pre-commit install
```

## Setup DB

1. Setup a DB server (PostgresSQL DB recommended) and populate your DB instance settings in the `.env` file.
2. Run migrations to setup all tables: `python manage.py migrate`
3. To rebuild the DB from scratch: `python manage.py loader REBUILD`
