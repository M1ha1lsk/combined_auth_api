from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi import Request, HTTPException, Depends
from auth_db.app.database import verify_session, get_db
from auth_db.app.main import get_user_role, check_session
from sqlalchemy.orm import Session
from pydantic import BaseModel
import json
import subprocess
from uuid import UUID
import uuid
from kafka_producer import send_purchase

app = FastAPI()

class Purchase(BaseModel):
    customer_id: int
    product_id: str
    price: float
    sell_id: UUID
    seller_id: int

class PurchaseRequest(BaseModel):
    product_id: str

class AddProductRequest(BaseModel):
    product_name: str
    product_description: str
    category: str
    price: float

@app.post("/init-products")
def init_products():
    result = subprocess.run(
        [
            "docker", "exec", "spark-master",
            "spark-submit",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
            "/app/spark_jobs/init_products_table.py"
        ],
        capture_output=True,
        text=True
    )

    return {
        "status": "ok" if result.returncode == 0 else "error",
        "stdout": result.stdout,
        "stderr": result.stderr
    }

@app.get("/products")
def get_products():
    try:
        result = subprocess.run([
            "docker", "exec", "spark-master",
            "spark-submit",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
            "/app/spark_jobs/list_products.py"
        ], capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            error_detail = result.stderr if result.stderr else "Unknown Spark error"
            raise HTTPException(
                status_code=500,
                detail=f"Spark job failed: {error_detail}"
            )

        try:
            json_str = next(line for line in result.stdout.splitlines() 
                          if line.strip().startswith(('[', '{')))
            return json.loads(json_str)
        except (StopIteration, json.JSONDecodeError) as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to parse Spark output: {str(e)}. Full output: {result.stdout[:500]}..."
            )

    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=500,
            detail="Spark job timed out"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )

@app.post("/purchase")
async def create_purchase(
    p: PurchaseRequest, 
    request: Request, 
    db: Session = Depends(get_db)
):
    role_response = await get_user_role(request, db)
    user_role = role_response["role"]
    
    if user_role not in ["customer"]:
        raise HTTPException(
            status_code=403,
            detail="Недостаточно прав для совершения покупки, вы не customer"
        )

    session_token = request.cookies.get("session_token")
    user_id = verify_session(session_token, db)

    result = subprocess.run([
        "docker", "exec", "spark-master",
        "spark-submit",
        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
        "/app/spark_jobs/list_products.py"
    ], capture_output=True, text=True)

    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Spark job failed:\n{result.stderr}"
        )

    try:
        lines = result.stdout.splitlines()
        json_lines = [line for line in lines if line.strip().startswith(('{', '['))]
        available_products = json.loads("\n".join(json_lines))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Invalid JSON from Spark: {str(e)}"
        )

    product_info = None
    for product in available_products:
        if product["product_id"] == p.product_id:
            product_info = product
            break

    if not product_info:
        raise HTTPException(
            status_code=400,
            detail=f"Product {p.product_id} not found"
        )

    sell_id = str(uuid.uuid4())
    
    purchase_data = {
        "customer_id": user_id,
        "product_id": p.product_id,
        "sell_id": sell_id,
        "price": product_info["price"],
        "seller_id": product_info["seller_id"]
    }

    send_purchase(purchase_data)

    return {
        "status": "ok",
        "customer_id": user_id,
        "product_id": p.product_id,
        "sell_id": sell_id,
        "price": product_info["price"],
        "seller_id": product_info["seller_id"]
    }

@app.post("/add_product")
async def add_product(
    product: AddProductRequest, 
    request: Request, 
    db: Session = Depends(get_db)
):
    role_response = await get_user_role(request, db)
    if role_response["role"] not in ["seller", "admin"]:
        raise HTTPException(status_code=403, detail="Only sellers and admins can add products")

    session_token = request.cookies.get("session_token")
    user_id = verify_session(session_token, db)
    product_id = str(uuid.uuid4())
    
    product_data = {
        "product_id": product_id,
        "product_name": product.product_name,
        "product_description": product.product_description,
        "category": product.category,
        "price": product.price,
        "seller_id": user_id
    }

    try:
        result = subprocess.run([
            "docker", "exec", "spark-master",
            "spark-submit",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
            "/app/spark_jobs/add_product.py",
            json.dumps(product_data)
        ], capture_output=True, text=True, timeout=30)

        output_lines = result.stdout.splitlines()
        json_response = None
        
        for line in output_lines:
            if line.strip().startswith('{') and line.strip().endswith('}'):
                try:
                    json_response = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue

        if result.returncode != 0 or not json_response:
            error_msg = result.stderr if result.stderr else "Unknown Spark error"
            raise HTTPException(
                status_code=500,
                detail=f"Spark job failed: {error_msg}"
            )

        return json_response

    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=500,
            detail= "Spark job timed out"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )