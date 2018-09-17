from requests import Session 
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
import os


class DefaultSession(Session):
    def __init__(self):
        super(DefaultSession, self).__init__()
        self.hortonworks = 'https://docs.hortonworks.com'
        self.hortonworks_docs_index = self.hortonworks + '/HDPDocuments/HDF3/HDF-3.1.1/index.html'
        self.nifi_pdf_urls = []

    @staticmethod
    def _build_soup(html):
        return BeautifulSoup(html, 'html.parser')

    @staticmethod
    def _build_filename(url):
        return url.split('/')[-1]

    def build_pdf_list(self):
        r = self.get(self.hortonworks_docs_index)
        r.raise_for_status()
        soup = self._build_soup(r.text)
        dataflows = soup.select_one('div #section-dataflow')
        books = dataflows.select('div.book')
        for book in books:
            title = book.select_one('div.title')
            anchor = title.select_one('a')
            if anchor:
                url = anchor.get('href')
                self.nifi_pdf_urls.append(self.hortonworks + url)

    def download_docs(self):
        path = os.path.dirname(__file__)
        docs = os.path.join(path, '../docs')
        if not os.path.exists(docs):
            os.mkdir(os.path.join(path, '../docs'))

        for url in self.nifi_pdf_urls:
            try:
                resp = self.get(url, stream=True)

                with open(os.path.join(docs, self._build_filename(url)), 'wb') as f:
                    f.write(resp.content)
            except ConnectionError:
                print('Could get document at: {}'.format(url))
                continue


def main():
    session = DefaultSession()
    session.build_pdf_list()
    session.download_docs()


if __name__ == '__main__':
    main()
