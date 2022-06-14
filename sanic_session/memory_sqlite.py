import asyncio
import os
import sqlite3
import time
from concurrent.futures import Executor, ThreadPoolExecutor
from enum import Enum
from multiprocessing.dummy import Process
from queue import Queue
from typing import Optional, Tuple, Union, List, Any, Callable

import ujson

from .base import BaseSessionInterface, default_sid_provider


class CursorState:
    def __init__(self, cursor: sqlite3.Cursor):
        self.last_row_id = cursor.lastrowid
        self.row_count = cursor.rowcount
        self.description = cursor.description

    def __repr__(self):
        return f'last_row_id = {self.last_row_id}, row_count = {self.row_count}, description = {self.description}'

    def __str__(self):
        return self.__repr__()


class Command(Enum):
    Close = 0b00000000
    Commit = 0b00000001
    RollBack = 0b00000010
    NoMore = 0b00000100
    Execute = 0b00010000
    ExecuteMany = 0b00010001
    SelectOne = 0b00100000
    Select = 0b00100001


class AsyncQuery:
    def __init__(
            self,
            command: Command,
            request: Optional[str] = None,
            arguments: Optional[Tuple] = None,
            response_queue: Optional[Queue] = None,
    ):
        self.cmd = command
        self.sql = request
        self.arg = arguments
        self.res = response_queue

    def __repr__(self):
        return f'cmd = {self.cmd}, sql = {self.sql}, arg = {self.arg}, res = {self.res}'

    def __str__(self):
        return self.__repr__()


class AsyncSqlite:
    def __init__(
            self,
            command: Command,
            request: Optional[str] = None,
            arguments: Optional[Tuple] = None,
            response_queue: Optional[Queue] = None,
    ):
        self.cmd = command
        self.sql = request
        self.arg = arguments
        self.res = response_queue

    def __repr__(self):
        return f'cmd = {self.cmd}, sql = {self.sql}, arg = {self.arg}, res = {self.res}'

    def __str__(self):
        return self.__repr__()


class SQLiteThread(Process):
    def __init__(self, filename: str, autocommit: bool = False, journal_mode: str = "WAL"):
        super(SQLiteThread, self).__init__()
        self.filename: str = filename
        self.autocommit: bool = autocommit
        self.journal_mode: str = journal_mode
        # use request queue of unlimited size
        self.reqs: Queue[AsyncQuery] = Queue()
        # self.setDaemon(True)  # python2.5-compatible
        self.start()

    def run(self):
        if self.autocommit:
            conn: sqlite3.Connection = sqlite3.connect(self.filename, isolation_level=None, check_same_thread=False)
        else:
            conn: sqlite3.Connection = sqlite3.connect(self.filename, check_same_thread=False)
        conn.execute('PRAGMA journal_mode = %s' % self.journal_mode)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        while 1:
            query: AsyncQuery = self.reqs.get()
            if query.cmd == Command.Close:
                break
            elif query.cmd == Command.Commit:
                conn.commit()
            elif query.sql is not None:
                sql: str = query.sql.strip()
                ret: Optional[Union[CursorState, BaseException]] = None
                try:
                    cursor.execute(sql, query.arg) if query.arg is not None else cursor.execute(sql)
                    if query.cmd in {Command.Execute, Command.ExecuteMany}:
                        ret = CursorState(cursor)
                except Exception as e:
                    ret = e
                if query.res is not None:
                    if ret is not None:
                        query.res.put(ret)
                    elif query.cmd == Command.SelectOne:
                        query.res.put(cursor.fetchone())
                    elif query.cmd == Command.Select:
                        for rec in cursor:
                            query.res.put(rec)
                        query.res.put(Command.NoMore)
        conn.close()

    def execute(self, req: str, arg: Optional[Tuple] = None) -> CursorState:
        query = AsyncQuery(Command.Execute, req, arg, Queue())
        self.reqs.put(query)
        ret: Union[CursorState, BaseException] = query.res.get()
        if isinstance(ret, BaseException):
            self.reqs.put(AsyncQuery(Command.RollBack))
            raise ret
        if self.autocommit:
            self.reqs.put(AsyncQuery(Command.Commit))
        return ret

    def execute_many(self, req: str, items: List[Tuple]) -> List[CursorState]:
        results: List[CursorState] = []
        queue = Queue()
        for item in items:
            query = AsyncQuery(Command.ExecuteMany, req, item, queue)
            self.reqs.put(query)
            ret: Union[CursorState, BaseException] = query.res.get()
            if isinstance(ret, BaseException):
                self.reqs.put(AsyncQuery(Command.RollBack))
                raise ret
            results.append(ret)
        if self.autocommit:
            self.reqs.put(AsyncQuery(Command.Commit))
        return results

    def select(self, req: str, arg: Optional[Tuple] = None):
        query: AsyncQuery = AsyncQuery(Command.Select, req, arg, Queue())
        self.reqs.put(query)
        while 1:
            ret: Union[Any, Command, BaseException] = query.res.get()
            if isinstance(ret, BaseException):
                raise ret
            if isinstance(ret, Command) and ret == Command.NoMore:
                break
            yield ret

    def select_one(self, req: str, arg: Optional[Tuple] = None):
        query: AsyncQuery = AsyncQuery(Command.SelectOne, req, arg, Queue())
        self.reqs.put(query)
        ret: Union[Any, BaseException] = query.res.get()
        if isinstance(ret, BaseException):
            raise ret
        return ret

    def commit(self):
        self.reqs.put(AsyncQuery(Command.Commit))

    def close(self):
        self.reqs.put(AsyncQuery(Command.Close))


class MemorySqliteSessionInterface(BaseSessionInterface):
    SELECT_SQL = '''
SELECT 
    `sid`, 
    `start`,
    `expiry`,
    `store`
FROM `session`
WHERE `sid` = ?;
        '''

    def __init__(
            self,
            domain: str = None,
            expiry: int = 2592000,
            httponly: bool = True,
            cookie_name: str = "session",  # PHPSESSID
            prefix: str = "session:",
            sessioncookie: bool = False,
            samesite: str = None,
            session_name="session",
            secure: bool = False,
            sid_provider: Callable[[None], str] = default_sid_provider,
    ):
        super().__init__(
            expiry=expiry,
            prefix=prefix,
            cookie_name=cookie_name,
            domain=domain,
            httponly=httponly,
            sessioncookie=sessioncookie,
            samesite=samesite,
            session_name=session_name,
            secure=secure,
            sid_provider=sid_provider,
        )
        self.db_thread = SQLiteThread(':memory')
        sql = '''
CREATE TABLE `session`(
  `sid` VARCHAR(255) UNIQUE,
  `start` TIMESTAMP, 
  `expiry` TIMESTAMP, 
  `store` TEXT
);
        '''
        self.db_thread.execute(sql)
        self.executor: Executor = ThreadPoolExecutor(os.cpu_count())

    async def _get_value(self, prefix, sid):
        session_id = self.prefix + sid
        rec = await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self.db_thread.select_one,
            self.SELECT_SQL,
            (session_id,)
        )
        if rec and time.time() <= rec[2]:
            return ujson.loads(rec[3])

    async def _delete_key(self, key):
        sql = '''DELETE FROM `session` WHERE `sid` = ?;'''
        await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self.db_thread.execute,
            sql,
        )
        self.db_thread.commit()

    async def _set_value(self, key, data):
        store = ujson.dumps(data)
        self.db_thread.execute('BEGIN;')
        rec = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.db_thread.select_one,
                self.SELECT_SQL, (key,)
        )
        if not rec:
            start = time.time()
            expiry = start + self.expiry
            sql = '''INSERT INTO `session` VALUES(?, ?, ?, ?);'''
            await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.db_thread.execute,
                sql, (key, start, expiry, store)
            )
        else:
            expiry = time.time() + self.expiry
            sql = '''UPDATE `session` SET `expiry`=?, `store`=? WHERE `sid` = ?'''
            await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.db_thread.execute,
                sql, (key, expiry, store)
            )
        self.db_thread.commit()


def __test__():
    pass


if __name__ == '__main__':
    __test__()
