import datetime
import json

expiry = datetime.datetime.now() + datetime.timedelta(seconds=5)
expiry.replace(tzinfo=datetime.timezone.utc)
expiry_str = expiry.isoformat("T") + "Z"

fake_token = "0" * 137
fake_id = "abcdefghijklmnopqrstuvwxyz.1234567890" * 37 + "." * 32

data = """
{
  "credential": {
    "access_token": "%s",
    "id_token": "%s",
    "token_expiry": "%s"
  }
}
""" % (
    fake_token,
    fake_id,
    expiry_str,
)

print(json.dumps(json.loads(data), indent=4))
