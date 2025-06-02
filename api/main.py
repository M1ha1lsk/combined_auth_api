from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi import Request, HTTPException, Depends
from auth_db.app.database import verify_session, get_db
from auth_db.app.main import get_user_role
from sqlalchemy.orm import Session
from pydantic import BaseModel
import json
import subprocess
from datetime import datetime
from uuid import UUID
import uuid
from utils.kafka_producer import send_purchase


app = FastAPI()

class Purchase(BaseModel):
    customer_id: int
    product_id: str
    price: float
    sell_id: UUID
    seller_id: int

class PurchaseRequest(BaseModel):
    product_id: str
    quantity: int

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
        "seller_id": product_info["seller_id"],
        "quantity": p.quantity
    }

    send_purchase(purchase_data)

    return {
        "status": "ok",
        "customer_id": user_id,
        "product_id": p.product_id,
        "sell_id": sell_id,
        "price": product_info["price"],
        "seller_id": product_info["seller_id"],
        "quantity": p.quantity
    }

@app.post("/add_product")
async def add_product(
    product: AddProductRequest, 
    request: Request, 
    db: Session = Depends(get_db)
):
    role_response = await get_user_role(request, db)
    if role_response["role"] not in ["seller"]:
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
            "/app/spark_jobs/add_product.py",
            json.dumps(product_data)
        ], capture_output=True, text=True, timeout=50)

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
    role_response = await get_user_role(request, db)
    if role_response["role"] != "seller":
        raise HTTPException(status_code=403, detail="Only sellers can update products")

    session_token = request.cookies.get("session_token")
    user_id = verify_session(session_token, db)

    result = subprocess.run([
        "docker", "exec", "spark-master",
        "spark-submit",
        "/app/spark_jobs/list_products.py"
    ], capture_output=True, text=True, timeout=30)

    if result.returncode != 0:
        return JSONResponse(status_code=500, content={
            "status": "error",
            "step": "list_products",
            "stderr": result.stderr,
            "stdout": result.stdout
        })

    try:
        products = json.loads("\n".join([
            line for line in result.stdout.splitlines()
            if line.strip().startswith(('{', '['))
        ]))
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "status": "error",
            "step": "parse_list_products_output",
            "error": str(e),
            "raw_output": result.stdout
        })

    product = next((p for p in products if p["product_id"] == update_data.product_id), None)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if product["seller_id"] != user_id:
        raise HTTPException(status_code=403, detail="You can only update your own products")

    updated_product = {
        "product_id": update_data.product_id,
        "product_name": update_data.product_name,
        "price": update_data.price,
        "product_description": product.get("product_description", ""),
        "category": product.get("category", ""),
        "seller_id": product.get("seller_id", user_id),
        "created_at": product.get("created_at"),
        "updated_at": datetime.utcnow().isoformat()
    }

    try:
        update_payload = json.dumps(updated_product)
    except Exception as e:
        return JSONResponse(status_code=400, content={
            "status": "error",
            "step": "json_dumps_updated_product",
            "error": str(e),
            "product_data": str(updated_product)
        })

    spark_update = subprocess.run([
        "docker", "exec", "spark-master",
        "spark-submit",
        "/app/spark_jobs/update_product.py",
        update_payload
    ], capture_output=True, text=True, timeout=30)

    if spark_update.returncode != 0:
        return JSONResponse(status_code=500, content={
            "status": "error",
            "step": "update_product",
            "stderr": spark_update.stderr,
            "stdout": spark_update.stdout,
            "payload": update_payload
        })

    try:
        spark_output = json.loads(spark_update.stdout)
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "status": "error",
            "step": "parse_update_product_output",
            "error": str(e),
            "raw_output": spark_update.stdout
        })

    return {
        "status": "ok",
        "updated_product": updated_product,
        "spark_output": spark_output
    }