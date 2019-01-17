import configparser
import os


class ConfigInstruments:

    def __init__(self, path):
        self.path = path or "settings.ini"

    def create_config(self):
        """
        Create a config file
        """
        config = configparser.ConfigParser()
        config.add_section("server")
        config.set("server", "host", "127.0.0.1")
        config.set("server", "port", "8888")
        config.add_section("mysql")
        config.set("mysql", "host", "127.0.0.1")
        config.set("mysql", "port", "3306")
        config.set("mysql", "user", "plnekit")
        config.set("mysql", "password", "r2xj2dv27p")
        config.set("mysql", "db", "peltier")
        config.add_section("storage")
        config.set("storage", "file_path", "/home/plnekit")
        config.set("storage", "file_name", "far_shelf.data")

        with open(self.path, "w") as config_file:
            config.write(config_file)

    def get_config(self):
        """
        Returns the config object
        """
        if not os.path.exists(self.path):
            self.create_config(self.path)

        config = configparser.ConfigParser()
        config.read(self.path)
        return config

    def get_setting(self, section, setting):
        """
        Print out a setting
        """
        config = self.get_config()
        value = config.get(section, setting)
        # msg = "{section} {setting} is {value}".format(
        #     section=section, setting=setting, value=value
        # )

        return value

