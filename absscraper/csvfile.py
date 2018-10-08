import csv


class CsvFile(object):

    """
    Helper class for reading and writing csv files.
    """

    def __init__(self, filepath):
        self.filepath = filepath

    def write_dict(self, input_list = None, overwrite = False):

        """
        Writes a list of dictionaries into csv file.
        :param input_list: list of dicts/OrderedDicts
        :param overwrite: boolean indicating whether to overwrite existing csv or append to it
        :return: None
        """

        open_flag = 'a'
        if overwrite:
            open_flag = 'w'

        if input_list is None:
            print("No data!")
            return None

        fields = input_list[0].keys()

        with open(self.filepath, open_flag) as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fields, delimiter=";")
            if overwrite:
                writer.writeheader()
            for row in input_list:
                writer.writerow(row)

    def read_dict(self):

        """
        Reads a given csv file.
        :return: list of dicts/OrderedDicts
        """

        entries = []
        with open(self.filepath, 'r') as input_file:
            reader = csv.DictReader(input_file, delimiter=";")
            for row in reader:
                entries.append(row)

        return entries
