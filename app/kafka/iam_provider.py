from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


class MSKTokenProvider:

    def token(self) -> str:
        token, _ = MSKAuthTokenProvider.generate_auth_token(
            region="ap-northeast-2",
        )

        return token