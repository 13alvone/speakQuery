#!/usr/bin/env python3

import re
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


class QueryParser:
    def __init__(self, _query):
        self.query, self.utf8_error = self.clean_and_summarize_utf8_errors(_query)
        self.main_operations = []
        self.join_placeholders = {}  # Store joins temporarily with placeholders
        self.term_placeholders = {}  # Store terms temporarily with placeholders
        self.regex_placeholders = {}  # Store regex strings temporarily with placeholders
        self.remove_comments()

    def remove_comments(self):
        self.query = re.sub(r'#.*?(\n|$)', '\n', self.query)  # Remove comments

    @staticmethod
    def clean_and_summarize_utf8_errors(_query):
        """
        Cleans the input string by removing characters that are not valid in UTF-8,
        specifically targeting unpaired surrogates or other invalid Unicode data.
        Args:
        - input_string: The input string to be cleaned.
        Returns:
        - A tuple of the cleaned string and a summary of the removal.
        """
        # Remove invalid characters (e.g., unpaired surrogates) directly
        cleaned_list = [char for char in _query.replace('“', '"') if not 0xD800 <= ord(char) <= 0xDFFF]
        cleaned_string = ''.join(cleaned_list)
        removed_count = len(_query) - len(cleaned_string) # Calculate the number of removed characters
        return cleaned_string, f"Removed {removed_count} invalid characters."

    def sanitize_query(self):
        self.query = re.sub(r'\s*=\s*', '=', self.query)  # Normalize all equal "=" occurrences
        self.query = re.sub(r'\s*!=\s*', '!=', self.query)  # Normalize all equal "=" occurrences
        self.query = re.sub(r'\s*\+\s*', '+', self.query)  # Normalize all plus "+" occurrences
        self.query = re.sub(r'\s*\-\s*', '-', self.query)  # Normalize all minus "-" occurrences
        self.query = re.sub(r'\s*\,\s*', ',', self.query)  # Normalize all comma "," occurrences
        self.query = re.sub(r'\|\s*', '| ', self.query)  # Normalize all pipe "|" occurrences
        self.query = re.sub(r'\[\s*', '[', self.query)  # Normalize bracket Entry from joins
        self.query = re.sub(r'\s*\]', ']', self.query)  # Normalize bracket Exit from joins
        self.query = re.sub(r'\(\s*', '(', self.query)  # Normalize parenthesis Left
        self.query = re.sub(r'\s*\)', ')', self.query)  # Normalize parenthesis Right
        self.query = re.sub(r'eval\s*', 'eval ', self.query) # Normalize spaces after eval
        self.query = re.sub(r'fields\s*', 'fields ', self.query)  # Normalize spaces after fields
        self.query = re.sub(r'rmfields\s*', 'rmfields ', self.query)  # Normalize spaces after rmfields
        self.query = re.sub(r'rex\s*', 'rex ', self.query)  # Normalize spaces after rex
        self.query = re.sub(r'regex\s*', 'regex ', self.query)  # Normalize spaces after regex
        self.query = re.sub(r'stats\s*', 'stats ', self.query)  # Normalize spaces after stats
        self.query = re.sub(r'sort\s*', 'sort ', self.query)  # Normalize spaces after sort
        self.query = re.sub(r'filter\s*', 'filter ', self.query)  # Normalize spaces after filter
        self.query = re.sub(r' and ', ' AND ', self.query)  # Normalize AND operatives
        self.query = re.sub(r' or ', ' OR ', self.query)  # Normalize OR operatives
        self.query = re.sub(r'\n\s*', '\n', self.query).lstrip('\n').rstrip('\n')  # Normalize tab + newline behavior

    def identify_and_replace_joins(self):
        join_pattern = r'\| join [^\[]+\[([^\]]+)\]'
        join_counter = 0  # Counter for join placeholders

        def replacement(match):
            nonlocal join_counter
            placeholder = f'{{JOIN_{join_counter}}}'
            self.join_placeholders[placeholder] = match.group(1)  # Store join query with placeholder
            join_counter += 1
            return '| ' + placeholder

        self.query = re.sub(join_pattern, replacement, self.query)

    def identify_and_replace_terms(self):
        term_pattern = r'"([^"]+)"'
        term_counter = 0  # Counter for join placeholders

        def replacement(match):
            nonlocal term_counter
            placeholder = f'{{TERM_{term_counter}}}'
            self.term_placeholders[placeholder] = match.group(1)  # Store join query with placeholder
            term_counter += 1
            return f'{placeholder}'

        self.query = re.sub(term_pattern, replacement, self.query)

    def identify_and_replace_regex_strings(self):
        regex_pattern = r'r`(.*)`'
        regex_counter = 0  # Counter for join placeholders

        def replacement(match):
            nonlocal regex_counter
            placeholder = f'{{REGEX_{regex_counter}}}'
            self.regex_placeholders[placeholder] = match.group(1)  # Store join query with placeholder
            regex_counter += 1
            return f'{placeholder}'

        self.query = re.sub(regex_pattern, replacement, self.query)

    def parse_operations(self):
        # Parse 'index' and 'source'
        pattern = r'pfile=(\S+)'
        pfile_matches = re.findall(pattern, self.query)
        if len(pfile_matches) != 1:
            raise f'[!] Multiple pfile calls cannot be present, but at least one must be. If multiple pfiles must , ' \
                  f'be called, please call pfile a SINGLE time and join the target filenames with a comma (,).'
        for op_value in pfile_matches:
            op_value = op_value.strip()
            self.main_operations.append(("pfile", op_value))

        # Parse 'WHERE clause, if present
        pattern = r'(WHERE) ((\S+)\s+?($|\n$|\n|\|)|\(.*?\))'
        matches = [(x[0], x[1]) for x in re.findall(pattern, self.query)]
        for op_type, op_value in matches:
            op_value = op_value.strip()
            self.main_operations.append((op_type, op_value))

        # Parse other operations including placeholders for joins
        start_pattern = r'\|\s(.*?)($|\n$|\n)'
        pre_matches = [x[0] for x in re.findall(start_pattern, self.query)]
        for pre_match in pre_matches:
            parts = pre_match.split(' ')
            self.main_operations.append((parts[0].strip(), ' '.join(parts[1:]).strip()))

    def parse(self):
        self.identify_and_replace_terms()
        self.identify_and_replace_regex_strings()
        self.identify_and_replace_joins()
        self.sanitize_query()
        self.parse_operations()

        # Now, handle the joins using the stored placeholders
        for placeholder, join_query in self.join_placeholders.items():
            # This is where you would parse each join query separately and process them
            logging.info(f"[i] Join placeholder: {placeholder}, Join query: {join_query}")
            # Placeholder for join processing logic


class JoinQueryParser(QueryParser):
    def __init__(self, _query):
        super().__init__(_query)

    def identify_and_replace_joins(self):
        # Explicitly override to prevent nested join processing within a join.
        logging.info("[i] Nested joins are not processed in JoinQueryParser.")

    def parse(self):
        # Call to identify and replace terms first, then sanitize query
        self.identify_and_replace_terms()
        self.sanitize_query()
        # Directly parse operations without identifying/replacing further joins
        self.parse_operations()


def format_main_query(_query):
    query_object = QueryParser(query)
    query_object.parse()
    return query_object


def format_join_query(_join_query):
    join_query_object = JoinQueryParser(_join_query)
    join_query_object.parse()
    return join_query_object


# Example usage
if __name__ == "__main__":
    query = """
    # This valid query shows how messy input is allowed, because similar to rust, it will be formatted anyways.
    pfile="some paraquet file","Other filename",filename_without_spaces,network_*,*_events"pfile 62",pfile6 WHERE (field22 !   =   "something" AND field54 !  =    34   ) # Notice the paraquet extension is assumed, but never required when pointing out the pfile directive.
    |   fields     name,age # Limit to only these two fields
    | eval other_field  = round(shit + othershit, 2)
      |    eval new_field=name_age 
    |stats values(*) count by name 
          | join field1 [  index=otherdb source=othertable | fields field1,  field2]
    |eval field1  =concat(name, field1, " Added Information")
    | join field2 [index=anotherdb source=anothertable | fields field3, field4    ]
    # Comment line
    | fields final_field "Other Field" *  # Notice that a wildcard essentially should include all of the rest of the fields, preserving the column order from left to right for only final_field and "Other Field".
    | eval addition_example = field1 + field2 + field3 + field345
    | eval sum_example = sum(addition_example,field2,field3, field345)
    | rmfields addition_example  # Remove the fmfields field after using it in the previous line's sum() function example.
    | eval subtraction_example = field1 - field2 - field3
    | eval multiply_example = mult(field1, field34, field345)
    | rex field=event_detailed_description r`https:\/\/site\.com(?<target_uri>\S+)` # Example extraction of a capture group. Regex strings here are identified by r`<regex_string>` enclosures.
    | regex bills_overdue  !=r`(?si)yes|for\ssure|help`  # Example of using regex to limit results to only a few case-insensitive strings
    | eval divide_example = field1/field2
    | eval   round_with_divide_example    = round(field/field2, 2)  # 2 places past the decimal rounding
    | eval complex_order_of_operations_example = (field34 + 45)/86400 + 80/4
    | sort DESC 100 event_count  # Example sort, but option ASC can be used for ascending 
    | eval     example_complex_str=    concat(field23, " is a part of ", field45, ".")
    | filter (isnotnull(bills_overdue) OR isnull(field45))  # OR and AND operations are supported here as filtering mechanisms and standard math comparisons can be used as well if objects are numeric types.
    """
    parser = QueryParser(query)
    parser.parse()

    print("\nParsed Main Operations:")
    for op in parser.main_operations:
        print(op)
