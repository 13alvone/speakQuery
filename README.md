<p align="center">
  <img src="logos/speakQueryLogoREV0.png" alt="SpeakQuery Logo" width="420">
</p>

# SpeakQuery

SpeakQuery is an experimental search and ingestion engine. It processes a custom query language over local SQLite and Parquet files. A Flask web UI exposes search capabilities while background workers execute scheduled queries and ingestion jobs.

> **Project ethos**
>
> SpeakQuery is intentionally designed as non–rent-seeking software.
> It exists to be transparent, inspectable, and useful on its own merits.
> The core engine is free by design and remains so permanently.
>
> SpeakQuery survives only through correctness, clarity, and trust—not artificial restrictions or gated capability.

## Hard requirements

- Python **3.12.x** is a **hard requirement** for the initial release due to native/C++ (pybind/extension) dependencies.
	- The project will track newer Python versions on a delayed schedule to prioritize correctness and stability.

## Project philosophy and authorship

SpeakQuery is authored infrastructure, not a productized service.

- The core engine is open, inspectable, and intended for real-world use.
- Attribution matters: professional credit enables accountability and future work.
- Commercial use is welcome; erasure of authorship is not.
- Power belongs in software that can be audited and challenged.

Licensing and attribution details are defined in the repository’s `LICENSE` and `NOTICE` files.

## Environment variables

SpeakQuery loads environment variables in this order:

1. If `ENV_PATH` is set and points to a file, it loads that file.
2. Otherwise, it loads `./.env` if present.
3. Otherwise, it relies on already-exported environment variables.

Required for the app to start:

- `SECRET_KEY` (Flask secret)

Optional (only required if you want email alerting to work at runtime):

- `SMTP_USER`
- `SMTP_PASSWORD`
- Optional overrides: `SMTP_SERVER` (default `smtp.gmail.com`), `SMTP_PORT` (default `587`), `SMTP_STARTTLS` (default `true`), `SMTP_FROM` (defaults to `SMTP_USER`)

Important behavior change (recent):
- The email alert module is **safe to import** even if SMTP variables are missing.
- SMTP variables are validated **only when an email send is attempted** (not during setup / import / DB init).

## Run with Docker

1. **Build the image**

	docker build --no-cache -t speakquery .

2. **Create and encrypt the environment file**

	mkdir -p input_repos/speakQuery
	chmod 700 input_repos/speakQuery
	cp .env.example input_repos/speakQuery/.env
	# Edit the file and set SECRET_KEY and admin credentials (and SMTP_* only if you want emailing)
	export MASTER_KEY=$(python - <<'PY'
	from cryptography.fernet import Fernet
	print(Fernet.generate_key().decode())
	PY
	)
	python utils/env_crypto.py encrypt input_repos/speakQuery/.env \
		input_repos/speakQuery/.env.enc
	rm input_repos/speakQuery/.env
	chmod 600 input_repos/speakQuery/.env.enc

3. **Start the container**

	python utils/env_crypto.py decrypt input_repos/speakQuery/.env.enc > /tmp/sq_env
	docker run -d --name speakquery --env-file /tmp/sq_env -p 5000:5000 \
		--restart unless-stopped speakquery
	rm /tmp/sq_env

### Docker Compose

For persistent development:

	docker compose up --build -d
	docker compose down   # stop services

Volumes defined in `docker-compose.yml` persist databases and index files.

## Quick start (host)

1. Clone the repository.

2. Run setup (Python 3.12.x required).

	./setup.sh --recreate-venv

	What setup does:
	- Enforces Python 3.12.x (will fail fast with OS-specific install hints if missing).
	- Creates `./env` and installs `requirements.txt` and `requirements-dev.txt` (unless `--skip-dev`).
	- Builds native/C++ components via `build_custom_components.py` (if present).
	- Ensures a usable `./.env` exists:
		- If `./.env` is missing, it creates it.
		- If `SECRET_KEY` is missing, it generates one and writes it to `./.env`.
	- Initializes databases by importing `initialize_database()`.

3. (Optional) Create and encrypt `input_repos/speakQuery/.env` as described in the Docker section if you want encrypted-at-rest env handling outside Docker.

4. Place ingestion scripts in `scheduled_input_scripts/` (see `example_dataframe_job.py`).

5. Launch all services with `./run_all.sh` and open `http://localhost:5000`.

6. Add scheduled jobs through `/set_script_schedule`:

	curl -X POST http://localhost:5000/set_script_schedule \
		-H 'Content-Type: application/json' \
		-d '{"repo_id":1,"script_name":"scheduled_input_scripts/example_dataframe_job.py",
			"cron_schedule":"0 * * * *","output_subdir":"daily",
			"overwrite":true,"ttl":3600}'

### setup.sh options

	./setup.sh --python /path/to/python3.12
	./setup.sh --venv-dir ./env
	./setup.sh --skip-dev
	./setup.sh --wheel-only
	./setup.sh --allow-source-builds
	./setup.sh --recreate-venv

Notes:
- `--wheel-only` is helpful on systems where compiling heavy dependencies is problematic.
- Native component builds may require `cmake` and a working compiler toolchain.

## Email alerting (runtime)

If you enable scheduled searches that email results, configure SMTP variables in your environment (or `.env` / decrypted env file used by Docker).

Minimum required to send email:

	SMTP_USER="your-smtp-username"
	SMTP_PASSWORD="your-smtp-password"

Optional overrides:

	SMTP_SERVER="smtp.gmail.com"
	SMTP_PORT="587"
	SMTP_STARTTLS="true"
	SMTP_FROM="your-from-address@example.com"

Important:
- Missing SMTP variables will not prevent setup or database initialization.
- Email sends will fail at runtime with a clear error if SMTP credentials are not set.

## PyCharm Remote Debugging

Remote debugging is available across the entire stack via a site-level attach hook. Refer to `docs/pycharm_debugging.md` for configuration steps, environment variables, Docker tips, and verification commands.

## Architecture

- `app.py` – Flask application that can optionally start the background engines.
- `query_engine` – executes saved searches and writes results to `executed_scheduled_searches/`.
- `scheduled_input_engine` – runs ingestion scripts defined in `scheduled_inputs.db` and prunes old indexes.
- C++ extensions in `functionality/cpp_index_call` and `functionality/cpp_datetime_parser` are built via `build_custom_components.py`.

## Authentication

Create an administrator:

	python app.py create-admin <username> <password> --token <api_token>

Include the API token in requests:

	curl -H "Authorization: Bearer <api_token>" \
		http://localhost:5000/api/saved_search

The `/users.html` page lets administrators create additional accounts.

## Testing

After activating your virtual environment:

	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	flake8 --exclude=env
	bandit -r .
	pytest -vv

`ci_setup.sh` performs these steps in continuous integration environments.

## Helper utilities

- `describe_indexes.py` – summarize index files under `indexes/`.
- `describe_frontend.sh` – report frontend environment information.
- `utils/generate_fake_dbs_and_data.py` – create sample Parquet data.

## Lookup file upload

The web UI accepts `.sqlite3`, `.parquet`, `.csv`, and `.json` files. CSV uploads are parsed using `csv.Sniffer` and `pandas.read_csv`. Files over 16 MB are rejected unless `LOOKUP_MAX_FILESIZE` is raised in `app.py`.

## Regenerating the parser

`lexers/speakQuery.g4` defines the grammar. Regenerate the parser with ANTLR:

	java -jar utils/antlr-4.13.1-complete.jar -Dlanguage=Python3 -no-listener -visitor \
		-o lexers/antlr4_active lexers/speakQuery.g4

The grammar omits increment/decrement operators, bitwise negation, semicolons and the `else` keyword.

## License and attribution

SpeakQuery is licensed under the Apache License, Version 2.0.

See `LICENSE` and `NOTICE` for full terms, attribution requirements, and authorship details.

## References

- Query syntax: `docs/syntax.md`
- API reference: `docs/api.md`
