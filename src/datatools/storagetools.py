import tables as pt
from os import path, getcwd


def type_check(data_list):
    data_type = None
    for item in data_list:
        # first time round just set the data type
        if not data_type:
            data_type = type(item)
        # if some types mismatch just set all to strings
        if data_type != type(item):
            print("string")
            return str
    # if all match then return the type
    print(data_type)
    return data_type


def dict_to_hdf5(data_dict, file_name, file_path=None):
    # check if file path is valid and open the file
    if file_path:
        assert path.exists(file_path), f"{file_path} is not a valid file path"
        file_name = f"{file_path}/{file_name}"
    else:
        file_name = f"{getcwd()}/{file_name}"

    h5file = pt.open_file(file_name, "w")

    # unpack the data in the dicitonary
    data_items = []
    value_length = None
    for key, value in data_dict.items():
        # make sure all the value items are list types
        if type(value) != list:
            pt.file._open_files.close_all()
            raise TypeError("one or more value items in the dictionary is not a list")
        # make sure all the key items are str types
        if type(key) != str:
            pt.file._open_files.close_all()
            raise TypeError("one or more key items in the dictionary is not a string")

        # checking if all lists have the same length
        if not value_length:
            value_length = len(value)
        if len(value) != value_length:
            pt.file._open_files.close_all()
            raise IndexError(
                "the lengths of one or more value items in the dictionary do not match"
            )

        # storing data in the data_list
        data_items.append((key, value))

    # Make the description for the table >> There is an error here somewhere check this: https://stackoverflow.com/questions/58261748/how-to-dynamically-assign-columns-to-a-pytables-isdescription-class
    class Description(pt.IsDescription):
        for col in data_items:
            # get type present in list
            type_to_set = type_check(col[1])

            # set column type acordingly
            if type_to_set == bool:
                exec(f"{col[0]} = pt.BoolCol()")
            elif type_to_set == int:
                print(f"{col[0]} = pt.Int64Col()")
                exec(f"{col[0]} = pt.Int64Col()")
            elif type_to_set == float:
                exec(f"{col[0]} = pt.Float64Col()")
            else:
                exec(f"{col[0]} = pt.StringCol()")

    # Create the table (group creation not implemented yet so always puts it in root) (might want to add table naming as well)
    table = h5file.create_table(h5file.root, "Table", Description)

    # Populate the table with data
    for idx in range(value_length):
        row = table.row

        # loop through all columns, the keys can now be used as column identifiers
        for col in data_items:
            row[col[0]] = col[1][idx]

        row.append()

    # Save table
    table.flush()

    ## SECTION FOR ATTRIBUTE ADDING (not implemented yet ...)

    # Closing the file
    pt.file._open_files.close_all()


file_path = f"{getcwd()}/Data"
file_name = "Test.h5"

test_dict = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}

# dict_to_hdf5(test_dict, file_name, file_path=file_path)
