from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.sasl.oauth import AbstractTokenProvider


class MSKTokenProvider(AbstractTokenProvider):
    """kafka-python OAUTHBEARER 인증에 사용할 AWS MSK IAM 토큰 제공자."""

    def token(self) -> str:
        token, _ = MSKAuthTokenProvider.generate_auth_token(
            region="ap-northeast-2",
        )

        return token
