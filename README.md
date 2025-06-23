# SpeakQuery

SpeakQuery is an experimental search and ingestion engine. The system processes a custom query language and operates over local SQLite and Parquet files. A Flask web interface exposes search features while background engines run scheduled queries and ingestion tasks.

## Architecture

- **`app.py`** – Flask application providing the web UI, upload handlers and query endpoints. It can optionally spawn the background engines.
- **`query_engine`** – Executes saved searches on a schedule. It reads tasks from `saved_searches.db`, runs each query via `CmdExecutionBackend` and stores results under `executed_scheduled_searches/`. The entry point is `crank_query_engine()` in `QueryEngine.py`.
- **`scheduled_input_engine`** – Runs scheduled ingestion scripts and cleans old indexes. Tasks are loaded from `scheduled_inputs.db`. Its entry point is `crank_scheduled_input_engine()` in `ScheduledInputEngine.py`.
- **C++ extensions** – Performance‑critical functions live in `functionality/cpp_index_call` and `functionality/cpp_datetime_parser`. These are compiled as Python modules.

## Setup

1. **Python version** – The project targets Python 3.11 as defined in `environment.yml`.
2. **Install dependencies**
   - Using `pip`:
     ```bash
     python3 -m venv env
     source env/bin/activate
     pip install -r requirements.txt
     ```
   - Using conda:
     ```bash
     conda env create -f environment.yml
     conda activate speakQueryEnv
     ```
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
5. **Run the application**
   - Start the Flask server (requires a `SECRET_KEY` environment variable):
     ```bash
     export SECRET_KEY="your_secret_value"
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

To enable email alerts, set your SMTP credentials as environment variables before running the query engine:

```bash
export SMTP_USER="your_email@example.com"
export SMTP_PASSWORD="your_app_password"
```

`SMTP_SERVER` and `SMTP_PORT` can also be specified to override the default Gmail settings.

## Testing

Install the Python requirements first, then run the security and unit tests located in `tests/`:
```bash
pip install -r requirements.txt
python build_custom_components.py --rebuild
flake8 --exclude=env
bandit -r .
pytest -vv
```
The script `tests/automated_build_test.sh` demonstrates compiling the extensions and running a small sample test in one step.

## Helper scripts

Two small utilities help inspect your data and server setup:

- **`describe_indexes.py`** – prints summary information about the index files
  stored under `indexes/`.
- **`describe_frontend.sh`** – outputs basic environment information helpful when
  debugging the web frontend.

## Timechart command

The query language includes a `timechart` directive for quick time-series
aggregation. Use `span=<interval>` to control bin size and optional `BY` fields
to group results. Example:

```spl
| timechart span=1h count by host
```


## Regenerating the parser

`lexers/speakQuery.g4` is the authoritative grammar for the query language. Parser files under `lexers/antlr4_active/` are generated from this grammar.
To regenerate them, place `antlr-4.13.1-complete.jar` in the `utils/` directory and run:

```bash
java -jar utils/antlr-4.13.1-complete.jar -Dlanguage=Python3 -no-listener -visitor \
  -o lexers/antlr4_active lexers/speakQuery.g4
```

The grammar intentionally omits tokens for increment/decrement operators
(`++`, `--`), bitwise negation (`~`), semicolons and the `else` keyword.
These constructs are not part of the supported query language.
