from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


class MSKTokenProvider(AbstractTokenProvider):
    """AWS MSK IAM 인증에 필요한 OAUTHBEARER 토큰을 kafka-python에 제공한다."""

    def token(self):
        # MSK 클러스터 리전 기준으로 짧은 수명의 IAM 인증 토큰을 매 연결 시점에 발급한다.
        token, _ = MSKAuthTokenProvider.generate_auth_token("ap-northeast-2")
        return token