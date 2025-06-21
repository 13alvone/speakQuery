#!/usr/bin/env python3

import logging
import collections
import antlr4.tree.Tree  # Assuming antlr4-python3-runtime is installed

logging.basicConfig(level=logging.INFO, format='%(message)s')


class StackHandler:
    def __init__(self):
        self.test = 'test'

    @staticmethod
    def convert_to_string(obj):
        """Convert TerminalNodeImpl or other types to string."""
        if isinstance(obj, antlr4.tree.Tree.TerminalNodeImpl):
            return obj.getText()  # Correct method to get string representation.
        else:
            return str(obj)

    def process_operations_in_list(self, target_list, operation_str, replacement_result):
        """Recursively process lists to find and replace matching operations."""
        # Check if the list contains only TerminalNodeImpl or stringifiable objects.
        if all(isinstance(x, (antlr4.tree.Tree.TerminalNodeImpl, str, int, float)) for x in target_list):
            concatenated = ''.join(self.convert_to_string(e) for e in target_list).replace(" ", "")
            if concatenated == operation_str:
                return replacement_result  # Return the replacement result instead of evaluating
            return target_list  # Return original list if no match found.
        else:  # Recursively process nested lists.
            for i, item in enumerate(target_list):
                if isinstance(item, list):
                    result = self.process_operations_in_list(item, operation_str, replacement_result)
                    target_list[i] = result  # Update the item with the processed result
            return target_list

    def process_ordered_dict(self, current_stack_od, operation_str, replacement_result):
        """Recursively process an OrderedDict for matching operations."""
        for key, value in current_stack_od.items():
            if isinstance(value, collections.OrderedDict):
                self.process_ordered_dict(value, operation_str, replacement_result)
            elif isinstance(value, list):
                result = self.process_operations_in_list(value, operation_str, replacement_result)
                current_stack_od[key] = result  # Update with the processed result

    @staticmethod
    def trim_stack_to_last_list(od):
        """
        Trims an OrderedDict by removing all entries after the last list entry.

        Parameters:
        - od: collections.OrderedDict to trim.

        Returns:
        - A trimmed OrderedDict.
        """
        # Create a reversed list of keys for iteration
        reversed_keys = list(reversed(od.keys()))
        found_list = False

        # Find the last list entry from the end
        for key in reversed_keys:
            if isinstance(od[key], list):
                found_list = True
                break
            if not found_list:
                # Remove the key if no list has been found yet
                del od[key]

        return od

    @staticmethod
    def trim_list_to_last_sublist(input_list):
        """
        Trims a list by removing all entries after the last sublist entry.

        Parameters:
        - input_list: List to trim.

        Returns:
        - A trimmed list.
        """
        # Find the index of the last sublist
        last_sublist_index = None
        for i in range(len(input_list) - 1, -1, -1):
            if isinstance(input_list[i], list):
                last_sublist_index = i
                break

        # If a sublist was found, trim the list to that point
        if last_sublist_index is not None:
            return input_list[:last_sublist_index + 1]
        else:
            # If no sublist was found, return the original list
            return input_list


def test():
    # Example replacement operation and result
    operation_str = "5++"
    replacement_result = 6  # Example replacement result

    data = collections.OrderedDict({
        "key1": [[5, "+", "+"], ["+", "+", 5], ["-", "-", 4], [4, "-", "-"], ["-", 5], ["~", 5], "otherValue"],
        "key2": collections.OrderedDict({
            "nestedKey1": ["5", "+", "+"],
            "nestedKey2": [[["+", "+", 5]]],  # Nested list example
            "nestedKey3": ["-", "-", 4],
            "nestedKey4": [4, "-", "-"],
            "nestedKey5": ["-", 5],
            "nestedKey6": ["~", 5]
        })
    })

    logging.info("[i] Original OrderedDict: {}".format(data))
    s_handler = StackHandler()
    s_handler.process_ordered_dict(data, operation_str, replacement_result)
    logging.info("[i] Processed OrderedDict: {}".format(data))


if __name__ == "__main__":
    test()
