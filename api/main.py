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
from api.utils.kafka_producer import send_purchase

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

class UpdateProductRequest(BaseModel):
    product_id: str
    product_name: str
    price: float

@app.post("/init-products")
async def init_products():
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
async def get_products():
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
    
@app.post("/update_product")
async def update_product(
    update_data: UpdateProductRequest,
    request: Request,
    db: Session = Depends(get_db)
):
    session_token = request.cookies.get("session_token")
    if not session_token:
        raise HTTPException(status_code=401, detail="Session token missing")
    
    user_id = verify_session(session_token, db)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid session token")

    role_response = await get_user_role(request, db)
    if role_response["role"] != "seller":
        raise HTTPException(status_code=403, detail="Only sellers can update products")

    try:
        product_check = subprocess.run([
            "docker", "exec", "spark-master",
            "spark-submit",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
            "/app/spark_jobs/get_product.py",
            update_data.product_id
        ], capture_output=True, text=True, timeout=60)

        if product_check.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch product: {product_check.stderr}"
            )

        try:
            product = json.loads(product_check.stdout)
            if not product:
                raise HTTPException(status_code=404, detail="Product not found")
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=500,
                detail="Invalid product data format from Spark"
            )

        if str(product["seller_id"]) != str(user_id):
            raise HTTPException(
                status_code=403,
                detail="You can only update your own products"
            )

        update_payload = {
            "product_id": update_data.product_id,
            "product_name": update_data.product_name,
            "price": update_data.price,
            "seller_id": user_id
        }

        spark_update = subprocess.run([
            "docker", "exec", "spark-master",
            "spark-submit",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
            "/app/spark_jobs/update_product.py",
            json.dumps(update_payload)
        ], capture_output=True, text=True, timeout=60)

        if spark_update.returncode != 0:
            error_msg = spark_update.stderr
            try:
                error_json = json.loads(spark_update.stderr)
                error_msg = error_json.get("message", error_msg)
            except:
                pass
            
            raise HTTPException(
                status_code=500,
                detail=f"Update failed: {error_msg}"
            )

        return {
            "status": "success",
            "product_id": update_data.product_id,
            "updated_fields": {
                "product_name": update_data.product_name,
                "price": update_data.price
            }
        }

    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=504,
            detail="Spark job timed out"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )
