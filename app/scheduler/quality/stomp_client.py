import stomp
import json
from dotenv import load_dotenv
import os

def run():
    load_dotenv()
    QUALITY_URL = os.getenv("QUALITY_URL")

    class AIListener(stomp.ConnectionListener):

        def on_message(self, frame):
            #data = json.loads(frame.body)
            #print("📩 수신 데이터:", data)

            # 👉 여기서 AI 로직 실행
            # risk_score 계산, anomaly detection 등


    conn = stomp.Connection([(QUALITY_URL, 8083)])

    conn.set_listener('', AIListener())

    conn.connect(wait=True)

    # Spring topic 구독
    conn.subscribe(destination='/topic/summary', id=1, ack='auto')
    conn.subscribe(destination='/topic/process', id=2, ack='auto')
    conn.subscribe(destination='/topic/drive-detail', id=3, ack='auto')
    conn.subscribe(destination='/topic/status-detail', id=4, ack='auto')

    print("🚀 AI-service WebSocket listening...")

    while True:
        pass

if __name__ == "__main__":
    run()