## 🚀 Запуск проекта  

1. **Создайте сеть**
   ```bash
   docker network create my_shared_net
   ```

2. **Установите зависимости:**  
   ```bash
   docker-compose up --build
   ```
3. **Откройте Swagger-UI:**  
   ```
   http://localhost:8000/docs/
   ```
4. **Логин и пароль админа:**  
   ```
   admin
   ```
   ```
   admin_pwd
   ```
5. **апишка**

   затем переходим на
   ```
   http://localhost:9001/
   ```
   логин и пароль оинаковый:
   ```
   minioadmin
   ```
   создаем бакет с названием 
   ```
   iceberg-warehouse
   ```
   переходим в сваггер
   ```
   http://localhost:8001/docs/
   ```
