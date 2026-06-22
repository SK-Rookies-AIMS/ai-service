from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


class MSKTokenProvider:

    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(
            region="ap-northeast-2"
        )

        return token