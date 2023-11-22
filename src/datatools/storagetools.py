import tables as pt
from os import path


def type_check(data_list):
    data_type = None
    for item in data_list:
        # first time round just set the data type
        if not data_type:
            data_type = type(item)
        # if some types mismatch just set all to strings
        if data_type != type(item):
            return str
    # if all match then return the type
    return data_type


def dict_to_hdf5(data_dict, file_path):
    # check if file path is valid and open the file
    assert path.exists(file_path), f"{file_path} is not a valid file path"
    h5file = pt.open_file(file_path, "w")

    # unpack the data in the dicitonary
    data_items = []
    value_length = None
    for key, value in data_dict.items():
        # make sure all the value items are list types
        if type(value) != list:
            raise TypeError("one or more value items in the dictionary is not a list")
        # make sure all the key items are str types
        if type(key) != str:
            raise TypeError("one or more key items in the dictionary is not a string")

        # checking if all lists have the same length
        if not value_length:
            value_length = len(value)
        if len(value) != value_length:
            raise IndexError(
                "the lengths of one or more value items in the dictionary do not match"
            )

        # storing data in the data_list
        data_items.append((key, value))

    # Make the description for the table
    class Description(pt.IsDescription):
        for col in data_items:
            # get type present in list
            type_to_set = type_check(col[1])

            # set column type acordingly
            if type_to_set == bool:
                exec(f"{col[0]} = pt.BoolCol()")
            elif type_to_set == int:
                exec(f"{col[0]} = pt.Int64Col()")
            elif type_to_set == float:
                exec(f"{col[0]} = pt.Float64Col()")
            else:
                exec(f"{col[0]} = pt.StringCol()")
