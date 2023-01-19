import boto3


class Boto3Client:

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):         # Foo 클래스 객체에 _instance 속성이 없다면
            cls._instance = super().__new__(cls)  # Foo 클래스의 객체를 생성하고 Foo._instance로 바인딩
        return cls._instance                      # Foo._instance를 리턴

    def __init__(
        self,
        aws_access_key_id: str, # aws access key id
        aws_secret_access_key: str, # aws secret access key
        aws_s3_name: str, # s3 bucket name
        aws_s3_region: str # s3 bucket region
    ):
        self.session = boto3.Session(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            region_name = aws_s3_region
        )
        self.s3_client = self.session.client('s3')
        self.s3_resource = self.session.resource('s3')
