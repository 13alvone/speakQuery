#!/usr/bin/env python3

import pandas as pd
from query_parser import QueryParser


def apply_stats_aggregations(df, stats_operations):
    for operation in stats_operations:
        # Parse the operation details
        operation_details = operation.split(' by ')
        aggregations = operation_details[0].split(', ')
        group_by_fields = operation_details[1].split() if len(operation_details) > 1 else []

        # Prepare for multiple aggregations
        agg_operations = {}
        for agg in aggregations:
            agg_func, agg_field = agg.split('(')
            agg_field = agg_field.replace(')', '')  # Remove closing parenthesis
            new_field_name = f"{agg_func}_{agg_field}"  # Default naming convention

            # Allow for custom naming via 'as'
            if ' as ' in agg:
                parts = agg.split(' as ')
                agg_func, agg_field = parts[0].split('(')
                agg_field = agg_field.replace(')', '')  # Remove closing parenthesis
                new_field_name = parts[1]

            # Map pandas aggregation functions
            if agg_func == 'count':
                agg_operations[new_field_name] = ('size')
            elif agg_func == 'sum':
                agg_operations[new_field_name] = (agg_field, 'sum')
            elif agg_func == 'avg':
                agg_operations[new_field_name] = (agg_field, 'mean')
            # Extend here for other aggregation types, e.g., 'min', 'max'

        # Apply grouped aggregation
        if agg_operations:
            if 'size' in agg_operations.values():  # Special handling for 'count'
                df_agg = df.groupby(group_by_fields).agg(
                    **{k: pd.NamedAgg(column=v if isinstance(v, str) else v[0], aggfunc='size' if v == 'size' else v[1])
                       for k, v in agg_operations.items()}).reset_index()
            else:
                df_agg = df.groupby(group_by_fields).agg(
                    **{k: pd.NamedAgg(column=v[0], aggfunc=v[1]) for k, v in agg_operations.items() if
                       v != 'size'}).reset_index()
        else:
            df_agg = df  # Fallback if no aggregation operations are defined
    return df_agg


def apply_join_operations(df_main, join_operations, execute_subquery):
    for join_condition in join_operations:
        join_field, subquery = join_condition
        df_join = execute_subquery(subquery)  # This function needs to be defined
        df_main = pd.merge(df_main, df_join, on=join_field, how='left')
    return df_main



def apply_eval_transformations(df, eval_operations):
    for operation in eval_operations:
        # Assuming operation is a string like 'new_field=existing_field+1'
        field, expression = operation.split('=', 1)
        df[field] = df.eval(expression)
    return df


def apply_field_operations(df, field_operations):
    for operation in field_operations:
        if 'include' in operation:
            df = df[[*operation['include']]]
        elif 'exclude' in operation:
            df = df.drop(columns=operation['exclude'])
    return df


class DataEngine:
    def __init__(self, query):
        self.query = query
        self.parser = QueryParser(query)
        self.parsed_components = self.parser.parse()

    def execute_operations(self, df, operations):
        for op_type, op_value in operations:
            if op_type == 'fields':
                df = apply_field_operations(df, op_value)
            elif op_type == 'eval':
                df = apply_eval_transformations(df, op_value)
            elif op_type == 'stats':
                df = apply_stats_aggregations(df, op_value)
            # Placeholder for other operation types, including joins
        return df

    def execute_query(self):
        # Initial data retrieval logic remains the same

        # Aggregate initial DataFrames if necessary
        main_df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

        # Collect and order operations
        operations = []
        for component in self.parsed_components:
            for operation in self.parsed_components[component]:
                operations.append((component, operation))
        # Sort or order operations based on their original sequence in the query
        # This step assumes operations have been collected in an order-preserving manner

        # Execute operations in order
        main_df = self.execute_operations(main_df, operations)

        return self, main_df





# Example usage
if __name__ == "__main__":
    query = "index=mydatabase source=mytable | fields name age | eval new_field=name_age | stats count by name"
    engine = DataEngine(query)
    result_df = engine.execute_query()
    print(result_df)
