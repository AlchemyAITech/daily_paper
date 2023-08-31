import collections
import re
from urllib.request import urlretrieve
import pandas as pd
import yaml
from arxiv import SortCriterion, SortOrder, Search, Result, Client
from typing import Dict, Generator, List, OrderedDict
import json, os, time
import logging
import argparse
from tqdm import tqdm

logger = logging.getLogger(__name__)

# t: str '%Y-%m-%d' example "2023-9-30"
def strp_time(t:str):
    if isinstance(t, str):
        return time.strptime(t, '%Y-%m-%d')
    return t

# compare time if t_start <= t <= t_end
def compare_time(t, t_start = None, t_end=None, strp=False):
    if strp:
        t = strp_time(t)
        t_start = strp_time(t_start)
        t_end = strp_time(t_end)
    # check type(t_start), type(t_end) -->  time.struct_time
    if t_start: assert isinstance(t_start, time.struct_time)
    if t_end: assert isinstance(t_end, time.struct_time)
    if t_start is None and t_end is None:
        return True
    elif t_start is None:
        return t <= t_end
    elif t_end is None:
        return t >= t_start
    else:
        return t <= t_end and t >= t_start

class ArxivSearch(Search):
    def results(self, offset: int = 0, s_time=None, e_time=None, strp=False) -> Generator[Result, None, None]:
        """
        Executes the specified search using a default arXiv API client.

        For info on default behavior, see `Client.__init__` and `Client.results`.
        """
        return ArxivClient().results(self, offset=offset, s_time=s_time, e_time=e_time, strp=strp)

class ArxivClient(Client):
    def results(self, search: Search, offset: int = 0, s_time=None, e_time=None, strp=False) -> Generator[Result, None, None]:
        total_results = search.max_results
        first_page = True

        while offset < total_results:
            page_size = min(self.page_size, search.max_results - offset)
            logger.info("Requesting {} results at offset {}".format(
                page_size,
                offset,
            ))
            page_url = self._format_url(search, offset, page_size)
            feed = self._parse_feed(page_url, first_page)
            if first_page:
                # NOTE: this is an ugly fix for a known bug. The totalresults
                # value is set to 1 for results with zero entries. If that API
                # bug is fixed, we can remove this conditional and always set
                # `total_results = min(...)`.
                if len(feed.entries) == 0:
                    logger.info("Got empty results; stopping generation")
                    total_results = 0
                else:
                    total_results = min(
                        total_results,
                        int(feed.feed.opensearch_totalresults)
                    )
                    logger.info("Got first page; {} of {} results available".format(
                        total_results,
                        search.max_results
                    ))
                # Subsequent pages are not the first page.
                first_page = False
            # Update offset for next request: account for received results.
            offset += len(feed.entries)
            # Yield query results until page is exhausted.
            for entry in feed.entries:
                # filter with time:
                if s_time or e_time:
                    if not compare_time(entry.published_parsed, t_end=e_time, strp=strp):
                        continue
                    elif not compare_time(entry.published_parsed, t_start=s_time, strp=strp):
                        total_results = 0
                        break
                try:
                    yield Result._from_feed_entry(entry)
                except Result.MissingFieldError:
                    logger.warning("Skipping partial result")
                    continue

class ArxivTool(object):
    def __init__(self, config):
        self._config(config)

    def _config(self, config_file):
        self.cfg = {}
        # 将 YAML 字符串写入文件
        with open(config_file, 'r', encoding='utf-8') as f:
            info = yaml.load(f.read(), Loader=yaml.FullLoader)

        # Search config
        self.cfg["query"] = info.get('query', None)
        self.cfg["id_list"] = info.get('id_list', [])
        a = info.get('max_results', None)
        self.cfg["max_results"] = float('inf') if a is None else a
        # s_time="2023-8-15", e_time="2023-8-20"
        self.cfg["s_time"] = info.get('s_time', None)
        self.cfg["e_time"] = info.get('e_time', None)
        self.cfg["offset"] = info.get('offset', 0)

        # save config
        # pdf config
        self.cfg["save_root"] = info.get('save_root', './')
        assert isinstance(self.cfg["save_root"], str)
        self.cfg["save_pdf"] = info.get('save_pdf', False)
        self.cfg["with_source"] = info.get('with_source', False)
        self.cfg["pdf_dir"] = os.path.join(self.cfg["save_root"], "pdf")
        # xlsx config
        self.cfg["save_xlsx"] = info.get('save_xlsx', False)
        self.cfg['xlsx_sorted'] = info.get('xlsx_sorted', None)
        self.cfg["xlsx_name"] = info.get('xlsx_name', 'Arxiv') + ".xlsx"
        self.cfg["xlsx_path"] = os.path.join(self.cfg["save_root"], self.cfg["xlsx_name"])
        # markdown config
        self.cfg["save_markdown"] = info.get('save_markdown', False)
        self.cfg['markdown_sorted'] = info.get('markdown_sorted', None)
        self.cfg["markdown_name"] = info.get('markdown_name', 'Arxiv') + ".md"
        self.cfg["markdown_path"] = os.path.join(self.cfg["save_root"], self.cfg["markdown_name"])

        # info config
        default_list = ['paper_id',             #   paper_id # 文章id
                        'paper_title',          #   paper_title # 文章标题
                        'paper_first_author',   #   paper_first_author # 文章的第一作者
                        'paper_all_author',     #   paper_all_author  # 文章的所有作者
                        'publish_time',         #   publish_time # 文章的发布时间
                        'update_time',          #   update_time # 文章的更新时间
                        'paper_summary',        #   paper_summary # 文章摘要
                        'paper_url',            #   paper_url # 文章url
                        'pdf_url',              #   pdf_url # 文章url
                        'comment',              #   comment # 解释
                        'doi',
                        'primary_category',     # 文章主方向
                        'categories']           # 文章涉及方向

        self.cfg["xlsx_list"] = info.get('xlsx_list', default_list)
        self.cfg["markdown_list"] = info.get('markdown_list', default_list)
        # check information in all list
        for info_i in self.cfg["xlsx_list"] + self.cfg["markdown_list"]:
            assert info_i in default_list , info_i

    def serch(self, **kwargs):
        sort_by = kwargs.get('sort_by', SortCriterion.SubmittedDate)
        sort_order = kwargs.get('sort_order', SortOrder.Descending)
        arxiv_search = ArxivSearch(query = self.cfg["query"],
                                    id_list = self.cfg["id_list"],
                                    max_results = self.cfg["max_results"],
                                    sort_by = sort_by,
                                    sort_order = sort_order)

        return arxiv_search

    def results(self, **kwargs):
        arxiv_search = self.serch(**kwargs)
        return arxiv_search.results(offset = self.cfg["offset"], s_time = self.cfg["s_time"], e_time = self.cfg["e_time"], strp = True)

    def format_res(self, **kwargs):
        info_format = []
        for result_i in self.results(**kwargs):
            info_i = dict(
                paper_id=result_i.get_short_id(),  # 文章id
                paper_title = result_i.title,  # 文章标题
                comment = result_i.comment,
                paper_url = result_i.entry_id,  # 文章url
                pdf_url = result_i.pdf_url,  # pdf url
                paper_summary = result_i.summary.replace("\n", ""),  # 文章摘要需要剔除格式
                paper_first_author = "{}".format(result_i.authors[0]),  # 文章的第一作者
                paper_all_author = ", ".join(["{}".format(name) for name in result_i.authors]),  # 文章的第一作者
                publish_time = "{}".format(result_i.published.date()),  # 文章的发布时间
                update_time = "{}".format(result_i.updated.date()),  # 文章的更新时间
                doi = "{}".format(result_i) if result_i.doi else "",
                primary_category = result_i.primary_category, # 文章主方向
                categories = ", ".join(result_i.categories)                # 文章所属方向
            )
            info_format.append(info_i)

        return info_format

    def check_info(self, info, info_list):
        info_check = []
        for info_i in info:
            tmp = collections.OrderedDict()
            for k in info_list:
                tmp[k] = info_i[k]
            info_check.append(tmp)
        return info_check

    def dict2pd(self, info, info_list, sorted=None):
        data = collections.OrderedDict()
        for k in info_list:
            data[k] = [info_i[k] for info_i in info]
        # convert to dataframe using from_dict method
        pd_data = pd.DataFrame.from_dict(data)
        if sorted and sorted in pd_data:
            pd_data = pd_data.sort_values(by=sorted).reset_index()
        return pd_data

    def save_xlsx(self, info, info_list):
        pd_data = self.dict2pd(info, info_list, sorted=self.cfg["xlsx_sorted"])
        os.makedirs(os.path.dirname(self.cfg["xlsx_path"]), exist_ok=True)
        pd_data.to_excel(self.cfg["xlsx_path"], index=True)

        print("xlsx: {}".format(self.cfg["xlsx_path"]))

    def save_markdown(self, info, info_list):
        pd_data = self.dict2pd(info, info_list, sorted=self.cfg["markdown_sorted"])

        md_data = pd_data.to_markdown()
        os.makedirs(os.path.dirname(self.cfg["markdown_path"]), exist_ok=True)
        with open(self.cfg["markdown_path"], "w", encoding="utf-8") as file:
            file.write(md_data)

        print("Markdown: {}".format(self.cfg["markdown_path"]))

    def _get_default_filename(self, res, extension="pdf") -> str:
        """
        A default `to_filename` function for the extension given.
        """
        nonempty_title = res['paper_title'] if res['paper_title'] else "UNTITLED"
        return '.'.join([
            res['paper_id'].replace("/", "_"),
            re.sub(r"[^\w]", "_", nonempty_title),
            extension
        ])

    def save_pdf(self, info):
        os.makedirs(self.cfg["pdf_dir"], exist_ok=True)
        for info_i in tqdm(info):
            filename = self._get_default_filename(info_i)
            pdf_path = os.path.join(self.cfg["pdf_dir"], filename)
            written_path, _ = urlretrieve(info_i["pdf_url"], pdf_path)

            # Bodge: construct the source URL from the PDF URL.
            if self.cfg["with_source"]:
                source_filename = self._get_default_filename(info_i, "tar.gz")
                source_path = os.path.join(self.cfg["pdf_dir"], source_filename)
                source_url = info_i["pdf_url"].replace('/pdf/', '/src/')
                written_path, _ = urlretrieve(source_url, source_path)
        print("PDF done. {}".format(self.cfg["pdf_dir"]))

    def __call__(self, *args, **kwargs):
        info = self.format_res()

        if self.cfg['save_xlsx'] and len(self.cfg['xlsx_list']):
            self.save_xlsx(info, self.cfg['xlsx_list'])

        if self.cfg['save_markdown'] and len(self.cfg['markdown_list']):
            self.save_markdown(info, self.cfg['markdown_list'])

        if self.cfg['save_pdf']:
            self.save_pdf(info)
        return


if __name__ == '__main__':
    # r".\configs\iccv_2023.yml"
    parser = argparse.ArgumentParser()
    parser.add_argument('cfg')
    args = parser.parse_args()
    a = ArxivTool(args.cfg)()