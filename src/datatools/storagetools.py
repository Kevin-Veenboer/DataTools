import tables as pt
from os import path


def dict_to_hdf5(data_dict, file_path):
    assert path.exists(file_path), f"{file_path} is not a valid file path"
    h5file = pt.open_file(file_path, "w")
