# SpeakQuery

SpeakQuery is an experimental search and ingestion engine. The system processes a custom query language and operates over local SQLite and Parquet files. A Flask web interface exposes search features while background engines run scheduled queries and ingestion tasks.

## Architecture

- **`app.py`** – Flask application providing the web UI, upload handlers and query endpoints. It can optionally spawn the background engines.
- **`query_engine`** – Executes saved searches on a schedule. It reads tasks from `saved_searches.db`, runs each query via `CmdExecutionBackend` and stores results under `executed_scheduled_searches/`. The entry point is `crank_query_engine()` in `QueryEngine.py`.
- **`scheduled_input_engine`** – Runs scheduled ingestion scripts and cleans old indexes. Tasks are loaded from `scheduled_inputs.db`. Its entry point is `crank_scheduled_input_engine()` in `ScheduledInputEngine.py`.
- **C++ extensions** – Performance‑critical functions live in `functionality/cpp_index_call` and `functionality/cpp_datetime_parser`. These are compiled as Python modules.

## Setup

1. **Python version** – The project targets Python 3.11 as defined in `config/environment.yml`.
2. **Install dependencies**
   - Using `pip`:
     ```bash
     python3 -m venv env
     source env/bin/activate
     pip install -r config/requirements.txt
     ```
   - Using conda:
     ```bash
     conda env create -f config/environment.yml
     conda activate speakQueryEnv
     ```
3. **Build the C++ extensions** – From the project root run `build_custom_components.py`.
   This script compiles the performance‑critical modules in a repeatable,
   durable way:
   ```bash
   python build_custom_components.py
   ```
   You can build a single component with `--component` or force a clean
   rebuild with `--rebuild`.
4. **Run the application**
   - Start the Flask server:
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

## Testing

After building the C++ extensions, run the unit tests located in `tests/`:
```bash
pytest -vv
```
The script `tests/automated_build_test.sh` demonstrates compiling the extensions and running the tests in one step.
