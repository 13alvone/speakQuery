import pandas as pd
import numpy as np
import logging

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


class CallEventStats:
    def __init__(self, dataframe):
        self.main_df = dataframe

    def execute_eventstats(self, parsed_command):
        named_aggs = {}
        by_clause = None
        count_op = None

        if parsed_command[0] == 'eventstats':
            parsed_command = parsed_command[1:]

        try:
            for elem in parsed_command:
                if isinstance(elem, list) and elem[0] == 'values':
                    _, _, col, _, _, alias = elem
                    if col not in self.main_df.columns:
                        raise ValueError(f"Column '{col}' does not exist in the DataFrame.")
                    named_aggs[alias] = pd.NamedAgg(column=col, aggfunc=self.unique_values)
                elif isinstance(elem, str) and elem == 'count':
                    count_op = 'count'  # Indicate that count operation is required
                elif elem == 'by':
                    by_clause_index = parsed_command.index('by') + 1
                    by_clause = parsed_command[by_clause_index]
                    if by_clause not in self.main_df.columns:
                        raise ValueError(f"Grouping column '{by_clause}' does not exist in the DataFrame.")
        except ValueError as e:
            logging.error(f"[x] Error: {e}")
            return None

        try:
            if by_clause:
                group_by_object = self.main_df.groupby(by_clause)
                if named_aggs:
                    # Perform aggregations as specified and merge the results
                    agg_result = group_by_object.agg(**named_aggs).reset_index()
                    self.main_df = pd.merge(self.main_df, agg_result, on=by_clause, how='left')
                if count_op:
                    # Apply count to each group and merge
                    self.main_df[count_op] = group_by_object.transform('size')
            else:
                # Perform ungrouped aggregations if no by_clause is specified
                result = self.main_df.agg({**named_aggs})
                for key, value in result.iteritems():
                    self.main_df[key] = value
                if count_op:
                    self.main_df[count_op] = len(self.main_df)
            return self.main_df
        except Exception as e:
            logging.error(f"[x] Error during aggregation: {str(e)}")
            return None

    @staticmethod
    def unique_values(series):
        return series.dropna().unique().tolist()

    @staticmethod
    def values(x):
        return x.dropna().unique().tolist()

    @staticmethod
    def count(x):
        return x.count()

    @staticmethod
    def latest(x):
        return x.max()

    @staticmethod
    def earliest(x):
        return x.min()

    @staticmethod
    def first(x):
        return x.iloc[0]

    @staticmethod
    def last(x):
        return x.iloc[-1]

    @staticmethod
    def dcount(x):
        return x.nunique()

    @staticmethod
    def round(x, decimals=0):
        return x.round(decimals)

    @staticmethod
    def min(x):
        return x.min()

    @staticmethod
    def max(x):
        return x.max()

    @staticmethod
    def avg(x):
        return x.mean()

    @staticmethod
    def sum(x):
        return x.sum()

    @staticmethod
    def range(x):
        return x.max() - x.min()

    @staticmethod
    def median(x):
        return x.median()

    @staticmethod
    def mode(x):
        modes = x.mode()
        return modes[0] if not modes.empty else np.nan

    @staticmethod
    def sqrt(x):
        return np.sqrt(x)

    @staticmethod
    def abs(x):
        return x.abs()

    @staticmethod
    def random(*args):
        # Depending on the implementation, return a single random value or a series
        return np.random.rand()

    # Define other necessary statistical methods here


def main():  # Example usage
    # Example DataFrame initialization
    data = {
        'TEST_rename': [347.0000, 18.0000, 426.0000, 123.0000],
        'header_2': [185, 609, 725, 807],
        'header_3': ['pass', 'remember', 'that', 'at'],
        't3t': ['test', 'test', 'test', 'test'],
        't4t': ['A Thing', 'A Thing', 'A Thing', 'A Thing']
    }
    df = pd.DataFrame(data)

    # Example parsed command input
    parsed_command = ['eventstats', ['values', '(', 'header_3', ')', 'as', 't2t'], 'count', 'by', 't4t']

    sq_stats = CallStats(df)
    result = sq_stats.execute_eventstats(parsed_command)
    logging.info(f"[i] Results:\n{result}")


if __name__ == "__main__":
    main()
