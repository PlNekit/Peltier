import asyncio
import threading
import mysql.connector
import datetime as dt
import re
import logging
import config_instruments as conf
from time import time, sleep
from json import loads, dumps
from tempfile import gettempdir
from os import path

CONFIG = conf.ConfigInstruments("settings.ini")


# TODO добавить классы в соответствии с паттерном Observer (для рассылки сообщений подключенным абонентам (клиентам))

class MyDaemons(threading.Thread):
    def __init__(self, name, sleep_time, func, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True
        self.sleep_time = sleep_time
        self.name = name
        self.func = func
        self.kwargs = kwargs
        self._stop_event = threading.Event()
        self.log = logging.getLogger('MyDaemons::%s' % name)

    def run(self):
        self.log.info(r"daemon {} started".format(self.name))
        while not self.stopped():
            if self.stopped():
                return
            sleep(self.sleep_time)
            result = self.func(**self.kwargs)
            self.log.debug("{}::function result::{} ".format(self.name, result))
            if result == r"daemon.stop":
                self.stop()

    def stop(self):
        self.log.info(r"daemon {} stopped".format(self.name))
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class Connector:
    def __init__(self):
        self.conn = mysql.connector.connect(host=CONFIG.get_setting('mysql', 'host'),
                                            port=CONFIG.get_setting('mysql', 'port'),
                                            user=CONFIG.get_setting('mysql', 'user'),
                                            password=CONFIG.get_setting('mysql', 'password'),
                                            db=CONFIG.get_setting('mysql', 'db'),
                                            autocommit=True)
        self.log = logging.getLogger('Connector')
        self.daemon = MyDaemons("Pinger", 30, self.ping)
        self.daemon.start()

    def ping(self):
        self.log.info(r"Connector: ping mysql server")
        if not self.conn.is_connected():
            logging.error(r"Connector: daemon: mysql connection lost")
            logging.info(r"Try to reconnect mysql server")
            try:
                self.conn.reconnect(attempts=1, delay=0)
            except mysql.connector.Error as err:
                logging.error("mysql ping error: {}".format(err))
        else:
            return "Mysql connection is active"

    def close(self):
        self.conn.close()

    # def __del__(self):
    #     self.conn.close()


class Query:

    def __init__(self, connector):
        self.connector = connector.conn
        self.log = logging.getLogger('Query')

    def insert_furnaces(self, **kwargs):
        cur = self.connector.cursor()
        cur.execute("""
            INSERT INTO `peltier`.`furnaces`(`furnace_number`,
                `first_temperature`, `second_temperature`,
                `first_voltage`, `second_voltage`,
                `isBurn_flag`, `timestamp`)
            VALUE(%(furnace_number)s,
                %(first_temperature)s, %(second_temperature)s,
                %(first_voltage)s, %(second_voltage)s,
                %(isBurn_flag)s, %(timestamp)s);
            """, self._make_dictionary("insert_furnaces", **kwargs))
        self.connector.commit()
        cur.close()

    def check_existence(self, **kwargs):
        cur = self.connector.cursor(buffered=True)
        cur.execute("""
            SELECT
                IF(EXISTS(
                    SELECT 1
                    FROM `peltier`.`furnaces`
                    WHERE `furnaces`.`furnace_number` = %(furnace_number)s
                        AND `furnaces`.`timestamp` = %(timestamp)s
                        # AND `furnaces`.`first_temperature` = %(first_temperature)s
                        # AND `furnaces`.`second_temperature` = %(second_temperature)s
                        # AND `furnaces`.`first_voltage` = %(first_voltage)s
                        # AND `furnaces`.`second_voltage` = %(second_voltage)s
                        # AND `furnaces`.`isBurn_flag` = %(isBurn_flag)s
                    LIMIT 1),
                1, 0) AS 'isExists_flag';
            """, self._make_dictionary("check_existence", **kwargs))
        result = cur.fetchall()
        self.connector.commit()
        cur.close()
        return result

    @staticmethod
    def _make_dictionary(query_name, **kwargs):
        result = {}
        if query_name.__eq__("insert_furnaces") or query_name.__eq__("check_existence"):
            result = {
                "furnace_number": kwargs["furnace_number"],
                "timestamp": kwargs["timestamp"],
                "first_temperature": kwargs["temperature_1"],
                "second_temperature": kwargs["temperature_2"],
                "first_voltage": kwargs["voltage_1"],
                "second_voltage": kwargs["voltage_2"],
                "isBurn_flag": kwargs["isBurn_flag"],
            }

        return result


class Storage:
    """Класс для хранения метрик"""

    log = logging.getLogger('Storage')

    def __init__(self, name, connector):
        # используем словарь для временного хранения метрик
        self._data = {}
        self.query = Query(connector)
        self.file_storage = FileStorage(CONFIG.get_setting('storage', 'file_path'),
                                        CONFIG.get_setting('storage', 'file_name'))
        self.json = JsonInstruments(name)
        self.cleaner = MyDaemons("Cleaner", 15, self.clean_storage)
        self.cleaner.start()

    def put(self, furnace_number, temperature_1, temperature_2, voltage_1, voltage_2, isBurn_flag):

        timestamp = str(dt.datetime.now())
        timestamp = timestamp[:timestamp.find(".")]
        logging.debug("storage.put: input_data: {}".format((furnace_number,
                                                           temperature_1,
                                                           temperature_2,
                                                           voltage_1,
                                                           voltage_2,
                                                           isBurn_flag)))
        try:
            self.query.insert_furnaces(furnace_number=furnace_number,
                                       timestamp=timestamp,
                                       temperature_1=temperature_1,
                                       temperature_2=temperature_2,
                                       voltage_1=voltage_1,
                                       voltage_2=voltage_2,
                                       isBurn_flag=isBurn_flag)
        except mysql.connector.Error as err:
            self.log.critical("Mysql error: {}".format(err))
            self.log.info("Saving data in json storage")
            self.json.put_in(str(furnace_number), str(timestamp),
                             (float(temperature_1), float(temperature_2),
                              float(voltage_1), float(voltage_2),
                              int(isBurn_flag)))

    def get(self, furnace_number):
        data = self.json.get_data()
        # вовзращаем нужную метрику если это не *
        if furnace_number != "*":
            data = {
                furnace_number: data.get(furnace_number, {})
            }
        return data

    def clean_storage(self):
        # подтягиваем данные во временное локальное хранилище (словарь)
        # пытаемся раскидать данные в базу
        # успешно раскиданные данные убираем из файла
        # подгружаем новые данные из файла
        # повторяем, пока локальное хранилище не опустеет

        self._data = self.json.get_data()
        self.log.info("Waiting mysql connection")
        if self.query.connector.is_connected():
            self.log.info("Checking json storage")
            self._data = self.json.get_data()
            if self._data:
                self.log.info("data for clean: {}".format(self._data))
                inserted_data = {}  # словарь данных, добавленных в БД

                # перебираем данные, полученные из json хранилища
                for furnace_number, value in self._data.items():
                    for timestamp, local_value in value.items():
                        try:
                            temperature_1, temperature_2, voltage_1, voltage_2, isBurn_flag = local_value
                        except ValueError as err:
                            return "error: {}".format(err)
                        self.log.debug("furnace_number: {}, "
                                       "timestamp: {}, "
                                       "values: {} {} {} {} {}".format(furnace_number,
                                                                       timestamp,
                                                                       temperature_1,
                                                                       temperature_2,
                                                                       voltage_1,
                                                                       voltage_2,
                                                                       isBurn_flag))

                        # проверяем есть ли текущие данные в словаре (перед их отправкой в БД)
                        # если данных нет, пробуем отправить в базу
                        checking_existance = (self.query.check_existence(furnace_number=furnace_number,
                                                                         timestamp=timestamp,
                                                                         temperature_1=temperature_1,
                                                                         temperature_2=temperature_2,
                                                                         voltage_1=voltage_1,
                                                                         voltage_2=voltage_2,
                                                                         isBurn_flag=isBurn_flag)[0][0])
                        print(checking_existance)
                        self.log.debug("checking existance result: {}".format(checking_existance))
                        if checking_existance == 0:
                            self.log.debug("data not exist")
                            try:
                                self.log.debug("making_insert")
                                self.query.insert_furnaces(furnace_number=furnace_number,
                                                           timestamp=timestamp,
                                                           temperature_1=temperature_1,
                                                           temperature_2=temperature_2,
                                                           voltage_1=voltage_1,
                                                           voltage_2=voltage_2,
                                                           isBurn_flag=isBurn_flag)
                            except mysql.connector.Error as err:
                                self.log.error("Mysql connection error: %s" % err)

                            # проверяем, добавились ли наши данные
                            self.log.debug("checking inserted data")
                            checking_existance = (self.query.check_existence(furnace_number=furnace_number,
                                                                             timestamp=timestamp,
                                                                             temperature_1=temperature_1,
                                                                             temperature_2=temperature_2,
                                                                             voltage_1=voltage_1,
                                                                             voltage_2=voltage_2,
                                                                             isBurn_flag=isBurn_flag)[0][0])
                            self.log.debug("checking existance result: {}".format(checking_existance))
                            if checking_existance == 1:
                                inserted_data[str(furnace_number)] = []
                                inserted_data[str(furnace_number)].append(timestamp)
                                self.log.debug("Данные загружены. Заполняем словарь")

                        elif checking_existance == 1:  # если такие данные уже есть в базе
                            inserted_data[str(furnace_number)] = []
                            inserted_data[str(furnace_number)].append(timestamp)
                            self.log.debug("Данные уже есть в базе. Заполняем словарь")

                self.log.debug("очищаем self._data: {}".format(inserted_data))
                for key, value in inserted_data.items():
                    for i in value:
                        if self._data.get(key).get(i):
                            if self._data[key].__len__() > 1:
                                self._data[key].pop(i)
                            else:
                                self._data.pop(key)
                self.log.info("data for overwrite")
                self.json.overwrite(self._data)
                return r"ok"
            else:
                return r"nothing to clean"
        else:
            return r"connection lost"

    def far_shelf(self, **kwargs):
        self.file_storage.put_in(kwargs["data"])


class JsonInstruments:
    def __init__(self, file_name):
        self.file_name = file_name + ".data"
        self.storage_path = path.join(gettempdir(), self.file_name)
        self.log = logging.getLogger('JsonInstruments %s' % self.file_name)

    def get_data(self):
        if not path.exists(self.storage_path):
            return {}

        with open(self.storage_path, 'r') as f:
            raw_data = f.read()
            if raw_data:
                return loads(raw_data)

            return {}

    def put_in(self, key, subkey, value):
        # подгружаем данные из файла
        data = self.get_data()

        # сверяем полученные данные и данные из файла на совпадение
        if key in data.keys():
            if subkey not in data[key].keys():
                data[key][subkey] = {}
                data[key][subkey] = value
        else:
            data[key] = {}
            data[key][subkey] = {}
            data[key][subkey] = value

        # пишем данные в файл
        with open(self.storage_path, 'w+') as f:
            f.write(dumps(data))
        self.log.info("Writing message into file by json storage: {}".format(data))

    def overwrite(self, data):
        # перезаписываем данные
        with open(self.storage_path, 'w') as f:
            f.write(dumps(data))
        self.log.info("Rewriting cleaned data into file by json storage: {}".format(data))


class FileStorage:

    def __init__(self, filepath, filename):
        self.filepath = filepath + "/" + filename
        self.log = logging.getLogger('FileStorage %s' % filename)

    def get_data(self):
        if not path.exists(self.filepath):
            logging.critical("FileStorage: get_data: Can't create path")
            return ""

        with open(self.filepath, 'rb') as f:
            raw_data = f.read()
            if raw_data:
                return raw_data

            return ""

    def put_in(self, input_string):
        data = self.get_data()

        if not isinstance(input_string, bytes):
            input_string = input_string.encode()

        input_string = input_string + b"\n"

        if not isinstance(data, bytes):
            data = data.encode()
        with open(self.filepath, 'wb+') as f:
            f.write(input_string + data)


class ParseError(ValueError):
    pass


class Parser:
    """Класс для реализации протокола"""

    PATTERN_put = re.compile(b">put\s[0-9\*]{1,3}(?:\s[0-9\.\+\-]{3,})+\s[0-9a-fA-F]{1,2}<")
    PATTERN_list = [PATTERN_put]

    def __init__(self):
        self.log = logging.getLogger('Parser')

    def cut(self, data):
        for pattern in self.PATTERN_list:
            message = pattern.search(data)
            if message:
                return message.group()

        return b""

    def encode(self, responses):
        """Преобразование ответа сервера в строку для передачи в сокет"""
        rows = []
        for response in responses:
            if not response:
                continue
            for furnace_number, timestamp_data in response.items():
                # print(f"response => furnace_number: {furnace_number}, timestamp_data: {timestamp_data}")
                for key, value in timestamp_data.items():
                    rows.append("{} {} {} {} {} {} {}".format(furnace_number,
                                                              key,
                                                              value[0],
                                                              value[1],
                                                              value[2],
                                                              value[3],
                                                              value[4]))

        result = "ok"

        if rows:
            result += " ".join(rows) + " "

        self.log.debug('Parser: encode: result: %s' % result)

        return result + "\n"

    def decode(self, data):
        """Разбор команды для дальнейшего выполнения. Возвращает список команд для выполнения"""
        parts = data.split("<")
        commands = []
        for part in parts:
            if not part:
                continue

            try:
                method, params = part.strip().split(" ", 1)
                if method == ">put":
                    furnace_number, temperature_1, temperature_2, voltage_1, voltage_2, isBurn_flag = params.split()
                    commands.append(
                        (method,
                         int(furnace_number),
                         float(temperature_1), float(temperature_2),
                         float(voltage_1), float(voltage_2),
                         int(isBurn_flag, 16))
                    )
                elif method == ">get":
                    key = params
                    commands.append(
                        (method, key)
                    )
                else:
                    raise ValueError("unknown method")
            except ValueError:
                raise ParseError("wrong command")
            self.log.debug('Parser: decode: commands: %s' % commands)
        return commands


class ExecutorError(Exception):
    pass


class Executor:
    """Класс Executor реализует метод run, который знает как выполнять команды сервера"""

    def __init__(self, storage):
        self.storage = storage

    def run(self, method, *params):
        if method == ">put":
            return self.storage.put(*params)

        elif method == ">get":
            return self.storage.get(*params)
        else:
            raise ExecutorError("Unsupported method")


class EchoServerClientProtocol(asyncio.Protocol):
    """Класс для реализции сервера при помощи asyncio"""

    connector = Connector()
    storage = Storage(name="peltier", connector=connector)

    def __init__(self):
        super().__init__()

        self.parser = Parser()
        self.query = Query(self.connector)
        self.executor = Executor(self.storage)
        self._buffer = b''
        self.log = logging.getLogger('EchoServerClientProtocol')

    def process_data(self, data):
        """Обработка входной команды сервера"""

        # разбираем сообщения при помощи self.parser
        try:
            commands = self.parser.decode(data)
        except ValueError:
            raise ParseError
        # выполняем команды и запоминаем результаты выполнения
        responses = []
        for command in commands:
            resp = self.executor.run(*command)
            responses.append(resp)
        self.log.info("Process data: responses: %s" % responses)
        # преобразовываем команды в строку
        return self.parser.encode(responses)

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        """Метод data_received вызывается при получении данных в сокете"""
        logging.info('Received %r from %s' % (data, addr))
        self._buffer += data
        self.storage.far_shelf(data=self._buffer)

        prepared_data = self.parser.cut(self._buffer)
        self.storage.far_shelf(data=prepared_data)
        try:
            decoded_data = prepared_data.decode()
        except UnicodeDecodeError as err:
            self.log.error('datagram_received: error %s' % err)
            return

        # ждем данных, если команда не завершена символом \n
        if not decoded_data.startswith('>') and not decoded_data.endswith('<'):
            self._buffer = b''
            return

        self._buffer = b''

        try:
            # обрабатываем поступивший запрос
            resp = self.process_data(decoded_data)
        except (ParseError, ExecutorError) as err:
            # формируем ошибку, в случае ожидаемых исключений
            self.transport.sendto("error;{}\n".format(err).encode(), addr)
            self.log.error('datagram_received: error %s' % err)
            return

        # формируем успешный ответ
        self.transport.sendto(resp.encode(), addr)
        self.log.info('Send message %r to %s' % (resp, addr))


def run_server(host, port):
    logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]{%(name)s}# %(levelname)-8s [%(asctime)s]  %(message)s',
                        level=logging.INFO)

    loop = asyncio.get_event_loop()
    logging.info("Starting UDP server")

    listen = loop.create_datagram_endpoint(
        EchoServerClientProtocol, local_addr=(host, port))
    transport, protocol = loop.run_until_complete(listen)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    transport.close()
    loop.close()


if __name__ == "__main__":
    run_server(CONFIG.get_setting('server', 'host'), CONFIG.get_setting('server', 'port'))
