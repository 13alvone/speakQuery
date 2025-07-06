# SpeakQuery


SpeakQuery is an experimental search and ingestion engine. The system processes a custom query language and operates over local SQLite and Parquet files. A Flask web interface exposes search features while background engines run scheduled queries and ingestion tasks.

## Docker (preferred)

The easiest way to run SpeakQuery is inside a Docker container.

1. Build the image:
   ```bash
   sudo docker build --no-cache -t speakquery .
   ```
2. Create a `.env` file and set at least the following variables:
   ```bash
   SECRET_KEY=change_me
   ADMIN_USERNAME=admin
   ADMIN_PASSWORD=admin
   ADMIN_ROLE=admin
   ADMIN_API_TOKEN=changeme
   SMTP_USER=you@example.com
   SMTP_PASSWORD=your_email_password
   SMTP_SERVER=smtp.gmail.com
   SMTP_PORT=587
   ```

3. Run the container:
   ```bash
   sudo docker run -d --name speakquery --env-file .env -p 5000:5000 --restart unless-stopped speakquery
   ```
   The container launches the web interface along with the background engines and will automatically restart unless stopped.

### Docker Compose

For persistent development use the provided compose file. Ensure a `.env` file exists
then start all services with:
```bash
sudo docker compose up --build -d
```
Volumes defined in `docker-compose.yml` keep database and index directories on the
host. Stop the services with:
```bash
sudo docker compose down
```

The manual setup steps below remain available for local development.

## Quick Start

1. Clone this repository.
2. Run `bash setup.sh` to install dependencies and initialize databases.
3. Copy `.env.example` to `.env` and edit the values for your environment.
4. Start all services with `./run_all.sh`.
5. Open a browser to the provided URL to use the web interface.
6. If the sidebar shows **No index files found or directory missing.**, create or
   mount index files under the configured `indexes` directory.

## Architecture

- **`app.py`** – Flask application providing the web UI, upload handlers and query endpoints. It can optionally spawn the background engines.
- **`query_engine`** – Executes saved searches on a schedule. It reads tasks from `saved_searches.db`, runs each query via `CmdExecutionBackend` and stores results under `executed_scheduled_searches/`. The entry point is `crank_query_engine()` in `QueryEngine.py`.
- **`scheduled_input_engine`** – Runs scheduled ingestion scripts and cleans old indexes. Tasks are loaded from `scheduled_inputs.db`. Its entry point is `crank_scheduled_input_engine()` in `ScheduledInputEngine.py`.
- **C++ extensions** – Performance‑critical functions live in `functionality/cpp_index_call` and `functionality/cpp_datetime_parser`. These are compiled as Python modules.

## Setup

1. **Python version** – The project targets Python 3.11 as defined in `environment.yml`.
2. **Run the setup script**
   The project ships with `setup.sh` which bootstraps a Python virtual
   environment, installs the requirements, builds the custom C++ components and
   initializes the application's databases:
   ```bash
   bash ./setup.sh
   ```
   If you prefer to manage the environment manually, create your own virtual
   environment and install the packages from `requirements.txt` and
   `requirements-dev.txt`.

### Platform-specific setup

If you are installing dependencies manually, the repository provides
platform-specific requirement files. Use these in addition to
`requirements.txt` and `requirements-dev.txt` and make sure the required build
tools are available:

| Platform | Python packages | Additional tools |
| -------- | --------------- | ---------------- |
| **Ubuntu** | `pip install -r ubuntu_requirements.txt` | `sudo apt install build-essential cmake` |
| **macOS** | `pip install -r macos_requirements.txt` | `brew install cmake` (Xcode or CLT should provide a C++17 compiler) |
3. **Required build tools**
   - CMake ≥3.5
   - A C++17 compiler
   - `pybind11` (the build script will attempt to install it automatically)
4. **Build the C++ extensions** – After activating the environment, run
   `build_custom_components.py` from the project root. The script compiles the
   performance‑critical modules and installs `pybind11` automatically if it is
   missing. To force a clean rebuild:
   ```bash
   python build_custom_components.py --rebuild
   ```
   You can also build a single component with `--component`.
5. **Create an environment file**
   Copy `.env.example` to `.env` and replace the placeholder values with your own.
   The application uses `python-dotenv` to load variables from this file:
   ```bash
   cp .env.example .env
   # edit .env to provide real credentials
   ```
6. **Run the application**
   - The recommended approach is to launch all services together using the helper script:
     ```bash
     ./run_all.sh
     ```
     This loads variables from `.env` and starts the Flask app, query engine and scheduled input engine. Press `Ctrl+C` to stop everything at once.

   - Alternatively you can start each component manually:
     - Start the Flask server (the `SECRET_KEY` loaded from `.env` is required):
       ```bash
       python app.py
       ```
     - Run the query engine (in a separate terminal):
       ```bash
       python query_engine/QueryEngine.py
       ```
     - Run the scheduled input engine:
       ```bash
       python scheduled_input_engine/ScheduledInputEngine.py
       ```

   These engines run continuously and can also be launched from within `app.py` by uncommenting `start_background_engines()`.

### SMTP configuration

Email alerts require SMTP credentials. Set these values in your `.env` file or export them before running the query engine:

```bash
export SMTP_USER="your_email@example.com"
export SMTP_PASSWORD="your_app_password"
```

`SMTP_SERVER` and `SMTP_PORT` can also be specified in `.env` or via environment variables to override the default Gmail settings.

### Concurrency and Rate Limiting

The Flask API processes requests through a small worker queue. By default only
two requests are executed concurrently and additional requests will wait until a
worker is free. API rate limiting is powered by **Flask-Limiter** and can be
enabled or disabled through the `THROTTLE_ENABLED` setting. Adjust the following
values in the settings database or via the `/update_settings` endpoint (requires
an admin login) to tune
performance:

- `QUEUE_SIZE` – maximum number of queued requests (default `20`)
- `PROCESSING_LIMIT` – number of worker threads (default `5`)
- `THROTTLE_ENABLED` – set to `true` or `false` to toggle per-IP throttling
- `BAN_DELETIONS_ENABLED` – enable IP banning after repeated failed DELETE requests

## Authentication

SpeakQuery uses a hybrid authentication system based on **Flask-Login**. When the application initializes an empty database, a default admin account is created. Credentials can be supplied as environment variables or via the CLI:

```bash
python app.py create-admin <username> <password> --token <api_token>
```

If `--token` is omitted a random value is generated. Set `ADMIN_USERNAME`, `ADMIN_PASSWORD`, `ADMIN_ROLE` and `ADMIN_API_TOKEN` in `.env` to control the initial admin user.

Include the API token in the `Authorization` header when calling the API:

```bash
curl -H "Authorization: Bearer <api_token>" http://localhost:5000/api/saved_search
```

Tokens are stored hashed. Re-run `create-admin` to rotate credentials or add new administrators.
Passwords are hashed securely using Werkzeug's `generate_password_hash`.

### Manual database initialization

If `/login` returns an error about a missing `users` table, create an administrator manually:

```bash
python app.py create-admin <username> <password> --token <api_token>
```

This command sets up the necessary tables and inserts the specified account so future logins succeed.


## Testing

After running `setup.sh` and activating the virtual environment, install the development
dependencies before executing the test suite. Packages such as
`werkzeug`, `pandas`, `aiosqlite`, `antlr4-python3-runtime`, and `PyYAML` are
required:
```bash
source env/bin/activate
pip install -r requirements-dev.txt
flake8 --exclude=env
bandit -r .
pytest -vv
```
The script `tests/automated_build_test.sh` demonstrates compiling the extensions
and running a small sample test in one step.

## Helper scripts

Two small utilities help inspect your data and server setup:

- **`describe_indexes.py`** – prints summary information about the index files
  stored under `indexes/`.
- **`describe_frontend.sh`** – outputs basic environment information helpful when
  debugging the web frontend.

To generate example indexes for testing, run:

```bash
python utils/generate_fake_dbs_and_data.py 1000 5
```
This creates a Parquet file under `indexes/test_parquets/` and logs the
full path when writing completes.

## Uploading lookup files

Lookup tables can be uploaded through the web UI. The frontend validates file
extensions before submitting the request:

```javascript
const allowedExtensions = ['sqlite3', 'parquet', 'csv', 'json'];
const fileExt = file.name.split('.').pop().toLowerCase();
if (!allowedExtensions.includes(fileExt)) {
    alert('Invalid file type');
    return;
}
```

The server performs an additional check when a `.csv` file is uploaded. The
`/upload_file` route tries to detect a valid delimiter with `csv.Sniffer` and
falls back to `pandas.read_csv` if necessary. Files that fail these checks are
rejected even if they use the `.csv` extension.

## Timechart command

The query language includes a `timechart` directive for quick time-series
aggregation. Use `span=<interval>` to control bin size and optional `BY` fields
to group results. Example:

```spl
| timechart span=1h count by host
```


## Regenerating the parser
`lexers/speakQuery.g4` is the authoritative grammar for the query language. Parser files under `lexers/antlr4_active/` are generated from this grammar. The Python runtime
for ANTLR (`antlr4-python3-runtime`) is required for both development and the test suite.
To regenerate them, place `antlr-4.13.1-complete.jar` in the `utils/` directory and run:

```bash
java -jar utils/antlr-4.13.1-complete.jar -Dlanguage=Python3 -no-listener -visitor \
  -o lexers/antlr4_active lexers/speakQuery.g4
```

The grammar intentionally omits tokens for increment/decrement operators
(`++`, `--`), bitwise negation (`~`), semicolons and the `else` keyword.
These constructs are not part of the supported query language.

## Syntax reference
See [docs/syntax.md](docs/syntax.md) for a summary of the query grammar and directive usage examples.

## API reference
See [docs/api.md](docs/api.md) for endpoint details and example commands.
