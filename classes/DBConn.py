import psycopg2
import asyncpg
import sqlalchemy


class DBConn:

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):         # Foo 클래스 객체에 _instance 속성이 없다면
            # print("DBConn __new__ is called\n")
            cls._instance = super().__new__(cls)  # Foo 클래스의 객체를 생성하고 Foo._instance로 바인딩
        return cls._instance                      # Foo._instance를 리턴

    def __init__(
        self,
        host: str,
        port: str,
        user: str,
        password: str,
        database: str
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        # print("DBConn __init__ is called\n")

    def getPsypgConn(self):
        try:
            conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
            )
            print("데이터베이스 접속 성공")
        except psycopg2.Error:
            print("데이터베이스 접속 실패. db정보를 확인하세요")
        return conn

    def getAlchmyConn(self):
        con = sqlalchemy.create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        con = con.execution_options(isolation_level="AUTOCOMMIT")
        return con

    async def getApgConn(self):
        return await asyncpg.connect(
            user=self.user,
            password=self.password,
            database=self.database,
            host=self.host,
            port=self.port,
        )