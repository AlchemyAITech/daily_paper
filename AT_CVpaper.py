import argparse
import re
import requests
import urllib.request
import os
import socket
from tqdm import tqdm

socket.setdefaulttimeout(30)

class CVP(object):
    def __init__(self, url, dst, dtype="CVPR"):
        self.url = url
        self.dst = dst
        assert dtype in ["CVPR", "ECCV", "ICCV", "WACV", "ACCV"]
        self.info_dict = self.get_url_info()
        self.num = len(self.info_dict)

    def get_url_info(self):
        r = requests.get(self.url)
        data = r.text
        link_list = re.findall(r"(?<=href=\").+?pdf(?=\">pdf)", data)
        name_list = re.findall(r"(?<=paper.html\">).+(?=</a>)", data)
        if len(link_list) != len(name_list):
            name_list = [os.path.basename(url) for url in link_list]
        info_dict = {}
        for i, file_name in enumerate(name_list):
            info_dict[link_list[i]] = re.sub("[:\"?/ ]", "_", file_name)
        return info_dict

    def download_pdf(self):
        os.makedirs(self.dst, exist_ok=True)

        with tqdm(total=self.num) as pbar:
            for url, file_name in self.info_dict.items():
                save_path = os.path.join(self.dst, file_name + '.pdf')
                if os.path.exists(save_path):
                    pbar.set_description("File exists")  # 进度条前加内容
                    pbar.set_postfix(file_name='{}'.format(file_name + '.pdf'))
                    pbar.update()
                    continue
                else:
                    # download pdf file
                    pbar.set_description("Downloading")  # 进度条前加内容
                    pbar.set_postfix(file_name='{}'.format(file_name + '.pdf'))
                    pbar.update()
                    try:
                        urllib.request.urlretrieve('http://openaccess.thecvf.com/' + url, filename=save_path)
                    except:
                        continue
        print("Finished!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # 'https://openaccess.thecvf.com/CVPR2023?day=all'
    parser.add_argument('url')
    parser.add_argument('dst')
    args = parser.parse_args()

    CVP(args.url, args.dst).download_pdf()