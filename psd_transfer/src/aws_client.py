from boto3.session import Session


def get_s3_client(profile_name):
    session = Session(profile_name=profile_name)
    client = session.client("s3")
    return client


pingpong_profile = "irep-bpm-dev-small"
negocia_profile = "negocia-image-dev"


negocia_s3_client = get_s3_client(negocia_profile)
pingpong_s3_client = get_s3_client(pingpong_profile)
