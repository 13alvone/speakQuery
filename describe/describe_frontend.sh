#!/usr/bin/env bash
#
# describe_frontend.sh
# 
# This script prints out the HTML, CSS, and JS files for one or more frontend components.
# By default, it only prints the files for "index" unless otherwise specified.
# Usage examples:
#   ./describe_frontend.sh           # Prints only the "index" component (html, js, css)
#   ./describe_frontend.sh -t "index,saved_search,saved_searches"
#   ./describe_frontend.sh -t "*"    # Prints all known components
#
# Basic error handling and usage instructions are included.

#######################################
# Print usage instructions.
#######################################
usage() {
  echo "[i] Usage: $0 [-t <targets>]"
  echo "    -t <targets>   Comma-separated list of components to describe."
  echo "                   If not provided, defaults to 'index'."
  echo "                   Pass '*' to describe all components."
  exit 1
}

#######################################
# Print the HTML, JS, and CSS for a given component name.
# Arguments:
#   1) Component name (e.g., 'index', 'saved_search', ...)
#######################################
print_component() {
  local component="$1"

  # Each component has the same structure: 
  #   templates/<component>.html
  #   static/js/<component>.js
  #   static/css/<component>.css
  #
  # We'll check if the files exist, then cat them with a label.
  # If they don't exist, we'll note that as well.

  local html_file="frontend/templates/${component}.html"
  local js_file="frontend/static/js/${component}.js"
  local css_file="frontend/static/css/${component}.css"

  # HTML
  if [[ -f "$html_file" ]]; then
    echo "[i] Bash CMD: cat $html_file"
    cat "$html_file"
  else
    echo "[!] File not found: $html_file"
  fi

  echo

  # JS
  if [[ -f "$js_file" ]]; then
    echo "[i] Bash CMD: cat $js_file"
    cat "$js_file"
  else
    echo "[!] File not found: $js_file"
  fi

  echo

  # CSS
  if [[ -f "$css_file" ]]; then
    echo "[i] Bash CMD: cat $css_file"
    cat "$css_file"
  else
    echo "[!] File not found: $css_file"
  fi

  echo
}

#######################################
# MAIN SCRIPT
#######################################

# Default targets if none provided
TARGETS="index"

# Parse arguments
while getopts ":t:" opt; do
  case "$opt" in
    t)
      TARGETS="$OPTARG"
      ;;
    \?)
      echo "[x] Invalid option: -$OPTARG" >&2
      usage
      ;;
    :)
      echo "[x] Option -$OPTARG requires an argument." >&2
      usage
      ;;
  esac
done

# Array of all known components
ALL_COMPONENTS=(
  "index"
  "saved_searches"
  "saved_search"
  "scheduled_inputs"
  "scheduled_input"
  "schedule_input"
  "history"
  "lookups"
)

# If user specified '*', override with all components
if [[ "$TARGETS" == "*" ]]; then
  COMPONENTS=("${ALL_COMPONENTS[@]}")
else
  # Otherwise, split user-provided CSV into an array
  IFS=',' read -r -a COMPONENTS <<< "$TARGETS"
fi

# Iterate and print each requested component
for comp in "${COMPONENTS[@]}"; do
  echo "[i] ----- DESCRIBING COMPONENT: $comp -----"
  print_component "$comp"
done

