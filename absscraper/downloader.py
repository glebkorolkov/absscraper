import requests


class FileDownloader(object):

    """
    Helper class for downloading large files.
    """

    @staticmethod
    def download(url, save_path):

        """
        Downloads and saves a single document.
        :param url: document url
        :param save_path: relative file path for saving the document
        :return: True is download was successful, False if unsuccessful
        """

        response = requests.get(url, stream=True)
        if not response.status_code == 200:
            print("Could not reach url: {}".format(url))
            return False

        with open(save_path, 'wb') as file_handle:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file_handle.write(chunk)

        print('Downloaded file {} to path "{}"...'.format(url, save_path))
        return True

    def preview_download(url):

        """
        Downloads first five lines of a file from provided url
        :return: string with first 5 lines of a file
        """

        response = requests.get(url, stream=True)
        if not response.status_code == 200:
            print("Could not reach url: {}".format(url))
            return None

        content_arr = []
        for i, chunk in enumerate(response.iter_content(chunk_size=1024, decode_unicode=True)):
            if i == 5:
                break
            content_arr.append(chunk)

        return "\n".join(content_arr)