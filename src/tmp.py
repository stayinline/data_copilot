import jwt
from config import JWT_SECRET, JWT_ALGORITHM

token = jwt.encode({"user_id": "test-user-001"}, JWT_SECRET, algorithm=JWT_ALGORITHM)
print(token)
# eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoidGVzdC11c2VyLTAwMSJ9.be5bzARYp_l6b2_tfIO1TPURndxMpf9xUXtmweI4JSM
