from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = (
    "mysql+pymysql://admin:K.d?S|46~($$z~.J2W)~W!aMEG)-"
    "@127.0.0.1:13306/sampledb"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT * FROM car_master LIMIT 1")
    )

    row = result.first()

    print(dict(row._mapping))

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT * FROM car_drive LIMIT 1")
    )

    row = result.first()

    print(dict(row._mapping))

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT * FROM car_dynamics LIMIT 1")
    )

    row = result.first()

    print(dict(row._mapping))

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT * FROM car_status LIMIT 1")
    )

    row = result.first()

    print(dict(row._mapping))

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT * FROM car_control LIMIT 1")
    )

    row = result.first()

    print(dict(row._mapping))