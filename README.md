# SpeakQuery

SpeakQuery is an experimental search and ingestion engine. It processes a custom
query language over local SQLite and Parquet files. A Flask web UI exposes search
capabilities while background workers execute scheduled queries and ingestion
jobs.

## Run with Docker

1. **Build the image**

   ```bash
   docker build --no-cache -t speakquery .
   ```

2. **Create and encrypt the environment file**

   ```bash
   mkdir -p input_repos/speakQuery
   chmod 700 input_repos/speakQuery
   cp .env.example input_repos/speakQuery/.env
   # edit the file and set SECRET_KEY and admin credentials
   export MASTER_KEY=$(python - <<'PY'
   from cryptography.fernet import Fernet
   print(Fernet.generate_key().decode())
   PY
   )
   python utils/env_crypto.py encrypt input_repos/speakQuery/.env \
       input_repos/speakQuery/.env.enc
   rm input_repos/speakQuery/.env
   chmod 600 input_repos/speakQuery/.env.enc
   ```

3. **Start the container**

   ```bash
   python utils/env_crypto.py decrypt input_repos/speakQuery/.env.enc > /tmp/sq_env
   docker run -d --name speakquery --env-file /tmp/sq_env -p 5000:5000 \
       --restart unless-stopped speakquery
   rm /tmp/sq_env
   ```

### Docker Compose

For persistent development:

```bash
docker compose up --build -d
docker compose down   # stop services
```

Volumes defined in `docker-compose.yml` persist databases and index files.

## Quick start (host)

1. Clone the repository and run `bash setup.sh`.
2. Create and encrypt `input_repos/speakQuery/.env` as above.
3. Place ingestion scripts in `scheduled_input_scripts/` (see
   `example_dataframe_job.py`).
4. Launch all services with `./run_all.sh` and open `http://localhost:5000`.
5. Add scheduled jobs through `/set_script_schedule`:

   ```bash
   curl -X POST http://localhost:5000/set_script_schedule \
        -H 'Content-Type: application/json' \
        -d '{"repo_id":1,"script_name":"scheduled_input_scripts/example_dataframe_job.py",
             "cron_schedule":"0 * * * *","output_subdir":"daily",
             "overwrite":true,"ttl":3600}'
   ```

## PyCharm Remote Debugging

Remote debugging is available across the entire stack via a site-level attach
hook.  Refer to [docs/pycharm_debugging.md](docs/pycharm_debugging.md) for
configuration steps, environment variables, Docker tips, and verification
commands.

## Architecture

- `app.py` – Flask application that can optionally start the background engines.
- `query_engine` – executes saved searches and writes results to
  `executed_scheduled_searches/`.
- `scheduled_input_engine` – runs ingestion scripts defined in
  `scheduled_inputs.db` and prunes old indexes.
- C++ extensions in `functionality/cpp_index_call` and
  `functionality/cpp_datetime_parser` are built via `build_custom_components.py`.

## Authentication

Create an administrator:

```bash
python app.py create-admin <username> <password> --token <api_token>
```

Include the API token in requests:

```bash
curl -H "Authorization: Bearer <api_token>" \
     http://localhost:5000/api/saved_search
```

The `/users.html` page lets administrators create additional accounts.

## Testing

After activating your virtual environment:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
flake8 --exclude=env
bandit -r .
pytest -vv
```

`ci_setup.sh` performs these steps in continuous integration environments.

## Helper utilities

- `describe_indexes.py` – summarize index files under `indexes/`.
- `describe_frontend.sh` – report frontend environment information.
- `utils/generate_fake_dbs_and_data.py` – create sample Parquet data.

## Lookup file upload

The web UI accepts `.sqlite3`, `.parquet`, `.csv`, and `.json` files. CSV uploads
are parsed using `csv.Sniffer` and `pandas.read_csv`. Files over 16 MB are
rejected unless `LOOKUP_MAX_FILESIZE` is raised in `app.py`.

## Timechart command

Example query:

```spl
| timechart span=1h count by host
```

## Regenerating the parser

`lexers/speakQuery.g4` defines the grammar. Regenerate the parser with ANTLR:

```bash
java -jar utils/antlr-4.13.1-complete.jar -Dlanguage=Python3 -no-listener -visitor \
  -o lexers/antlr4_active lexers/speakQuery.g4
```

The grammar omits increment/decrement operators, bitwise negation, semicolons and
the `else` keyword.

## References

- Query syntax: [docs/syntax.md](docs/syntax.md)
- API reference: [docs/api.md](docs/api.md)

