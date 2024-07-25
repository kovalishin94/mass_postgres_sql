import psycopg2
import paramiko
from services import log
from sshtunnel import SSHTunnelForwarder


class Server:
    def __init__(
            self,
            db_host: str,
            db_name: str,
            db_user: str,
            db_password: str,
            db_port: int = 5432,
            name: str = "NoName",
            **kwargs) -> None:

        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.name = name

        for key, value in kwargs.items():
            log(f"Unexpected keyword argument {key}={value} for {self.name} in config.yaml")

    def __str__(self) -> str:
        return self.name
    
    def __repr__(self) -> str:
        return self.name

    def connect_db(self):
        params = {
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
            "host": self.db_host,
            "port": self.db_port
        }

        return psycopg2.connect(**params)

    def execute_sql(self) -> list:
        result = []
        try:
            connection = self.connect_db()
            cursor = connection.cursor()

            cursor.execute(self.read_sql_txt())
            result = self.serialize_data(cursor)

            connection.close()
        except Exception as e:
            log(f"SQL error: {e}")

        return result

    @staticmethod
    def read_sql_txt() -> str:
        try:
            with open("sql.txt", "r", encoding="UTF-8") as sql_file:
                sql = sql_file.read()
            return sql
        except Exception as e:
            raise Exception(f"Cannot read sql-file because: {e}")

    @staticmethod
    def serialize_data(cursor) -> list:
        columns = [col[0] for col in cursor.description]
        query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return query_data


class ServerSSH(Server):
    def __init__(
            self,
            ssh_host: str,
            ssh_user: str,
            db_name: str,
            db_user: str,
            db_password: str,
            ssh_port: int = 22,
            db_host: str = "localhost",
            db_port: int = 5432,
            ssh_password: str = None,
            ssh_key_file_path: str = None,
            ssh_passphrase: str = None,
            **kwargs) -> None:

        super().__init__(db_host, db_name, db_user, db_password, db_port, **kwargs)

        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password
        self.ssh_key_file_path = ssh_key_file_path
        self.ssh_passphrase = ssh_passphrase

    def make_ssh_config(self) -> dict:
        config = {
            "ssh_address_or_host": (self.ssh_host, self.ssh_port),
            "ssh_username": self.ssh_user,
            "remote_bind_address": (self.db_host, self.db_port)
        }
        if self.ssh_password:
            config["ssh_password"] = self.ssh_password

        elif self.ssh_key_file_path:
            config["ssh_pkey"] = paramiko.RSAKey.from_private_key_file(
                self.ssh_key_file_path, self.ssh_passphrase)

        else:
            raise Exception("Not enough parameters to connect. \
                            You must specify the password or key from the ssh")

        return config

    def get_tunnel(self) -> SSHTunnelForwarder:
        try:
            ssh_config = self.make_ssh_config()
            tunnel = SSHTunnelForwarder(**ssh_config)
            tunnel.start()
            self.db_port = tunnel.local_bind_port
            self.db_host = "localhost"
            return tunnel
        except Exception as e:
            raise Exception(f"SSH error with '{self.name}' because: {e}")

    def execute_sql(self) -> list[Server]:
        result = []
        try:
            tunnel = self.get_tunnel()
            result = super().execute_sql()
            tunnel.close()
        except Exception as e:
            log(f"{e}")
        return result
     

def get_server_list(config: dict) -> list[Server]:
    sever_list = []
    for server in config.keys():
        if not config[server].get('database'):
            log(f"Missing required argument 'database' for '{server}' in config.yaml")

        params = config[server].get('database')
        try:
            if config[server].get('ssh'):
                params.update(config[server].get('ssh'))
                sever_list.append(ServerSSH(**params, name=server))
                log(f"config for {server} is correct")
                continue

            sever_list.append(Server(**params, name=server))
            log(f"config for {server} is correct")

        except TypeError as e:
            log(f"Missing required argument{e.__str__().split(":")[1]} for {server} in config.yaml")
        except Exception as e:
            log(f"Unexpected error: {e}")
    return sever_list
