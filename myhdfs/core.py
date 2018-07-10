from hdfs import InsecureClient

from .config import *
from .errors import *


class HdfsFile:
    '''HDFS File Object

    Keyword arguments:
        path -- HDFS file path
        mode -- one of ['r', 'rb', 'w', 'wb', 'a', 'ab'], 
                all else will be seen as 'w'
                (default 'r')
        encoding -- should be specified if not in binary mode 
                    (default 'utf-8')
    '''

    def __init__(self, path: str, mode: str='r', encoding: str='utf-8',
                 host: str=HDFS_HOST, port: int=HDFS_PORT, user: str=HDFS_USER):
        self.client = InsecureClient(url=f'http://{host}:{port}', user=user)

        self.path = path
        self.name = path.split('\\')[-1]
        self.mode = mode
        self.encoding = encoding

        if self.mode[0] == 'r':
            self.__cache_content()
            self.fptr = 0
        elif self.mode[0] == 'w':
            self.content = self.__binary_helper('')
            self.fptr = 0
        elif self.mode[0] == 'a':
            self.__cache_content()
            self.fptr = len(self.content)
        else:
            raise UnsupportedMode(f'unsupported mode {self.mode}')

    def __binary_helper(self, content):
        if len(self.mode) > 1 and self.mode[1] == 'b':
            if isinstance(content, str):
                return content.encode(self.encoding)
        else:
            if isinstance(content, bytes):
                return content.decode(self.encoding)
        return content

    def __cache_content(self):
        if not self.exists():
            raise FileNotFound()

        with self.client.read(self.path) as reader:
            self.content = self.__binary_helper(reader.read())

    # iterable compatible

    def __iter__(self):
        return self

    # iterator compatible
    def __next__(self):
        buffer = self.readline()
        if buffer == self.__binary_helper(''):
            raise StopIteration()
        return buffer

    # for with ... as ... use
    def __enter__(self):
        return self

    # for with ... as ... use
    def __exit__(self, type, value, traceback):
        self.flush()

    def exists(self) -> bool:
        if self.client.status(hdfs_path=self.path, strict=False) is None:
            return False
        return True

    def read(self, size: int=None) -> str or bytes:
        if self.mode[0] != 'r':
            raise UnsupportedOperation(f'{self.mode} does not support read')

        if size is None or size < 0:
            offset = len(self.content) - self.fptr
        else:
            offset = size
        buffer = self.content[self.fptr: self.fptr + offset]
        self.fptr += offset
        return buffer

    def readline(self, size: int=None) -> str or bytes:
        if self.mode[0] != 'r':
            raise UnsupportedOperation(f'{self.mode} does not support read')

        offset = 0
        while self.fptr + offset < len(self.content) and self.content[self.fptr + offset] not in [10, '\n']:
            offset += 1
        offset += 1
        buffer = self.content[self.fptr: self.fptr + offset]
        self.fptr += offset
        return buffer

    def seek(self, cookie: int):
        if not isinstance(cookie, int) or cookie < 0:
            raise InvalidParameterValue(f'cookie must be a non-negative integer')

        self.fptr = cookie

    def write(self, text: str or bytes) -> int:
        if self.mode[0] not in ['w', 'a']:
            raise UnsupportedOperation(f'{self.mode} does not support write')

        self.content += self.__binary_helper(text)
        return len(text)

    def flush(self):
        if self.mode[0] in ['w', 'a']:
            if not self.exists():
                self.client.write(hdfs_path=self.path, data=self.content)
            else:
                self.client.write(hdfs_path=self.path, data=self.content, overwrite=True)

    def close(self):
        self.flush()
