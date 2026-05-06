
from app.database import engine
from sqlalchemy import text

def test_conn():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print(f"Connection successful: {result.fetchone()}")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_conn()
