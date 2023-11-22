import tables as pt
from os import path


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

    class Description(pt.IsDescription):
        pass
