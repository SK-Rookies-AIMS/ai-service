from sqlalchemy import create_engine, text
from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)
print(os.getenv("OPENAI_API_KEY"))
DATABASE_URL = (
    "mysql+pymysql://admin:K.d?S|46~($$z~.J2W)~W!aMEG)-"
    "@127.0.0.1:13306/sampledb"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)
models = client.models.list()

for model in models.data:
    print(model.id)

def calculate_risk(status, control):

    score = 100
    reasons = []

    speed = float(status["speed"])
    rpm = int(status["att"])
    battery = float(status["battery_voltage"])

    if speed > 120:
        score -= 20
        reasons.append("과속")

    if rpm > 4000:
        score -= 20
        reasons.append("고RPM")

    if battery < 12.0:
        score -= 10
        reasons.append("배터리전압낮음")

    if control["collision_warning"] == 1:
        score -= 40
        reasons.append("충돌경고")

    if status["gear"] == "P" and speed > 20:
        score -= 30
        reasons.append("주행중 P기어")

    return score, reasons


def generate_comment(vehicle_id, score, reasons):

    prompt = f"""
차량번호: {vehicle_id}

위험점수: {score}

검출 이슈:
{', '.join(reasons)}

정비사가 작성하는 것처럼
2줄 이내로 진단 결과를 작성하세요.
"""

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {
                "role": "system",
                "content": "당신은 자동차 품질검사 전문가입니다."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return response.choices[0].message.content

with engine.connect() as conn:
    # 상태 데이터 1건 조회
    status_row = conn.execute(
        text("""
        SELECT *
        FROM car_status
        LIMIT 1
        """)
    ).first()

    # 제어 데이터 1건 조회
    control_row = conn.execute(
        text("""
        SELECT *
        FROM car_control
        LIMIT 1
        """)
    ).first()

    status_row = dict(status_row._mapping)
    control_row = dict(control_row._mapping)

    score, reasons = calculate_risk(
        status_row,
        control_row
    )

    comment = generate_comment(
        status_row["vehicle_id"],
        score,
        reasons
    )

    inspection_result = {
        "vehicle_id": status_row["vehicle_id"],
        "inspection_score": score,
        "inspection_result":
            "FAIL" if score < 70 else
            "WARN" if score < 90 else
            "PASS",
        "inspection_comment": comment
    }

    print(inspection_result)