from fastapi import FastAPI, Request, Form, Depends, HTTPException, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import os
import csv
import json
from datetime import datetime
import io
import tempfile
from pathlib import Path

import database as db_module
db = db_module.db

app = FastAPI(title="DB Admin App", version="1.0.0")

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== ТЕСТОВЫЙ ЭНДПОИНТ ====================
@app.get("/test")
async def test_endpoint():
    return {"message": "App is running", "status": "OK", "timestamp": datetime.now().isoformat()}

# ==================== СТАТИЧЕСКИЕ ФАЙЛЫ ====================
# Создаем папки если их нет
try:
    static_dir = Path("static")
    static_dir.mkdir(exist_ok=True)
    templates_dir = Path("templates")
    templates_dir.mkdir(exist_ok=True)
except Exception as e:
    print(f"Warning creating directories: {e}")

# Монтируем статические файлы
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception as e:
    print(f"Warning mounting static: {e}")

# Настраиваем шаблоны
templates = Jinja2Templates(directory="templates")

# ==================== ГЛАВНАЯ СТРАНИЦА ====================
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    tables = db.get_tables() or []
    table_counts = {}
    for table in tables:
        table_counts[table] = db.get_table_count(table)
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "tables": tables,
        "table_counts": table_counts
    })

# ==================== ФОРМЫ ДЛЯ ОПЕРАЦИЙ С ДАННЫМИ ====================
@app.get("/data", response_class=HTMLResponse)
async def data_forms(request: Request, table: str = "", page: int = 1):
    tables = db.get_tables() or []
    columns = []
    data = []
    total_count = 0
    total_pages = 0
    per_page = 200
    
    if table and table in tables:
        columns = db.get_table_columns(table) or []
        total_count = db.get_table_count(table)
        total_pages = (total_count + per_page - 1) // per_page
        
        if page < 1:
            page = 1
        if page > total_pages:
            page = total_pages
            
        offset = (page - 1) * per_page
        data = db.get_table_data(table, limit=per_page, offset=offset) or []
    
    return templates.TemplateResponse("data_forms.html", {
        "request": request,
        "tables": tables,
        "current_table": table,
        "columns": columns,
        "data": data,
        "page": page,
        "per_page": per_page,
        "total_count": total_count,
        "total_pages": total_pages
    })

@app.post("/api/data/insert")
async def insert_data(
    table: str = Form(...),
    data: str = Form(...)
):
    try:
        data_dict = json.loads(data)
        result = db.insert_data(table, data_dict)
        if result:
            return {"success": True, "message": f"Добавлена запись с ID: {result}", "id": result}
        else:
            return {"success": False, "error": "Не удалось добавить запись"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/data/update")
async def update_data(
    table: str = Form(...),
    data: str = Form(...),
    condition: str = Form(...)
):
    try:
        data_dict = json.loads(data)
        # Фильтруем пустые значения
        filtered_data = {k: v for k, v in data_dict.items() if v is not None and v != ''}
        
        if not filtered_data:
            return {"success": False, "error": "Нет данных для обновления"}
        
        result = db.update_data(table, filtered_data, condition)
        if result:
            return {"success": True, "message": "Данные обновлены с учетом связанных таблиц"}
        elif result is False:
            return {"success": False, "error": "Не найдены записи для обновления"}
        else:
            return {"success": False, "error": "Ошибка при обновлении данных"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/data/delete")
async def delete_data(
    table: str = Form(...),
    condition: str = Form(...),
    cascade: bool = Form(False)  # Добавляем параметр каскадного удаления
):
    try:
        if not condition or condition.strip() == "":
            return {"success": False, "error": "Условие не может быть пустым"}
        
        if cascade:
            # Каскадное удаление
            result = db.delete_data(table, condition)
            if result:
                return {"success": True, "message": "Данные удалены с учетом связанных таблиц"}
            else:
                return {"success": False, "error": "Ошибка при каскадном удалении"}
        else:
            # Безопасное удаление (проверка зависимостей)
            result = db.delete_data_safe(table, condition)
            if isinstance(result, dict):
                if result.get('success'):
                    return {
                        "success": True, 
                        "message": f"Удалено записей: {result.get('affected_rows', 0)}"
                    }
                else:
                    if result.get('error') == 'Есть зависимые записи':
                        dependencies = result.get('dependencies', [])
                        dep_message = "\n".join([f"- {d['table']}: {d['count']} записей" for d in dependencies])
                        return {
                            "success": False, 
                            "error": f"Нельзя удалить записи, так как есть связанные данные:\n{dep_message}\n\nИспользуйте каскадное удаление.",
                            "has_dependencies": True,
                            "dependencies": dependencies
                        }
                    else:
                        return {"success": False, "error": result.get('error', 'Неизвестная ошибка')}
            else:
                return {"success": False, "error": "Ошибка при удалении данных"}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==================== УДАЛЕНИЕ ТАБЛИЦ ====================
@app.post("/api/table/delete")
async def delete_table(table: str = Form(...)):
    """Удалить таблицу"""
    try:
        if not table:
            return {"success": False, "error": "Не указана таблица"}
        
        result = db.drop_table(table)
        if result:
            return {"success": True, "message": f"Таблица '{table}' удалена"}
        else:
            return {"success": False, "error": f"Не удалось удалить таблицу '{table}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==================== ЭКСПОРТ ОТДЕЛЬНОЙ ТАБЛИЦЫ ====================
@app.get("/api/export/table/{table_name}/{format}")
async def export_table(table_name: str, format: str):
    """Экспорт отдельной таблицы"""
    try:
        if format == "excel":
            filepath, filename = db.export_table_to_excel(table_name)
            if filepath:
                return FileResponse(
                    filepath,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=filename
                )
            else:
                return {"success": False, "error": filename}  # filename содержит сообщение об ошибке
        
        elif format == "json":
            filepath, filename = db.export_table_to_json(table_name)
            if filepath:
                return FileResponse(
                    filepath,
                    media_type="application/json",
                    filename=filename
                )
            else:
                return {"success": False, "error": filename}  # filename содержит сообщение об ошибке
        
        else:
            return {"success": False, "error": "Неподдерживаемый формат экспорта"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==================== КОНСТРУКТОР ЗАПРОСОВ ====================
@app.get("/query", response_class=HTMLResponse)
async def query_builder(request: Request):
    tables = db.get_tables() or []
    return templates.TemplateResponse("query_builder.html", {
        "request": request,
        "tables": tables
    })

@app.post("/api/query/execute")
async def execute_query(
    sql: str = Form(...),
    params: str = Form("")
):
    """Выполнить SQL запрос с параметрами"""
    try:
        # Парсим параметры
        params_dict = {}
        if params and params.strip():
            try:
                params_dict = json.loads(params)
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"Ошибка в формате параметров JSON: {str(e)}"}
        
        # Выполняем запрос
        result = db.execute_query(sql, params_dict)
        
        return {
            "success": True,
            "data": result,
            "count": len(result) if result else 0
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/query/export")
async def export_query_result(
    sql: str = Form(...),
    params: str = Form(""),
    format: str = Form("csv")
):
    """Экспорт результатов SQL запроса"""
    try:
        # Парсим параметры
        params_dict = {}
        if params and params.strip():
            try:
                params_dict = json.loads(params)
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"Ошибка в формате параметров JSON: {str(e)}"}
        
        # Выполняем запрос
        result = db.execute_query(sql, params_dict)
        
        if not result:
            return {"success": False, "error": "Нет данных для экспорта"}
        
        if format == "csv":
            # Используем новый метод через временную таблицу
            filepath, error = db.export_query_to_csv(result)
            
            if filepath:
                filename = f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                return FileResponse(
                    filepath,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=filename
                )
            else:
                return {"success": False, "error": error or "Ошибка при создании файла"}
        
        elif format == "json":
            # Создаем JSON
            json_data = json.dumps(result, ensure_ascii=False, indent=2, default=str)
            filename = f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            return JSONResponse({
                "success": True,
                "filename": filename,
                "content": json_data,
                "format": "json"
            })
        
        else:
            return {"success": False, "error": f"Неизвестный формат: {format}"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==================== ЭКСПОРТ ДАННЫХ ====================
@app.post("/api/export/tables")
async def export_selected_tables(
    tables: List[str] = Form(...),
    format: str = Form("excel")
):
    """Экспорт выбранных таблиц"""
    try:
        if not tables:
            return {"success": False, "error": "Не выбраны таблицы для экспорта"}
        
        if format == "excel":
            filepath, error = db.export_tables_to_excel(tables)
            if filepath:
                return FileResponse(
                    filepath,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=Path(filepath).name
                )
            else:
                return {"success": False, "error": error}
        
        elif format == "json":
            filepath, error = db.export_tables_to_json(tables)
            if filepath:
                return FileResponse(
                    filepath,
                    media_type="application/json",
                    filename=Path(filepath).name
                )
            else:
                return {"success": False, "error": error}
        
        else:
            return {"success": False, "error": "Неподдерживаемый формат экспорта"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/export/all/{format}")
async def export_all_tables(format: str):
    """Экспорт всех таблиц"""
    try:
        if format == "excel":
            filepath, error = db.export_all_to_excel()
            if filepath:
                return FileResponse(
                    filepath,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=Path(filepath).name
                )
            else:
                return {"success": False, "error": error}
        
        elif format == "json":
            filepath, error = db.export_all_to_json()
            if filepath:
                return FileResponse(
                    filepath,
                    media_type="application/json",
                    filename=Path(filepath).name
                )
            else:
                return {"success": False, "error": error}
        
        else:
            return {"success": False, "error": "Неподдерживаемый формат экспорта"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==================== СЕРВИСНЫЕ ФУНКЦИИ ====================
@app.get("/service", response_class=HTMLResponse)
async def service_page(request: Request):
    tables = db.get_tables() or []
    return templates.TemplateResponse("service.html", {
        "request": request,
        "tables": tables
    })

@app.post("/api/service/backup")
async def create_backup():
    """Создание полного бэкапа базы данных"""
    try:
        success, backup_file, error = db.create_backup()
        
        if success:
            return {
                "success": True,
                "message": f"Бэкап создан: {backup_file}",
                "file": backup_file
            }
        else:
            return {"success": False, "error": error}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/service/restore")
async def restore_backup(file: UploadFile = File(...)):
    """Восстановление БД из файла .backup"""
    try:
        if not file.filename:
            return {"success": False, "error": "Файл не выбран"}
        
        # Проверяем расширение файла
        if not file.filename.lower().endswith('.backup'):
            return {"success": False, "error": "Поддерживаются только файлы .backup"}
        
        # Сохраняем файл во временную директорию
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, file.filename)
        
        with open(temp_file_path, 'wb') as f:
            content = await file.read()
            f.write(content)
        
        # Восстанавливаем БД
        success, message = db.restore_backup(temp_file_path)
        
        # Удаляем временный файл
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        if success:
            return {
                "success": True,
                "message": message + " - страница обновится через 3 секунды..."
            }
        else:
            # Проверяем, если ошибка только из-за transaction_timeout
            if "unrecognized configuration parameter \"transaction_timeout\"" in message:
                return {
                    "success": True,
                    "message": "Восстановление выполнено с игнорированием предупреждений - страница обновится через 3 секунды..."
                }
            return {
                "success": False,
                "error": message
            }
            
    except Exception as e:
        # Удаляем временный файл при исключении
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        return {"success": False, "error": str(e)}

@app.post("/api/service/archive")
async def archive_tables(
    tables: str = Form("[]"),  # Изменено: получаем как строку JSON
    archive_all: bool = Form(False)
):
    """Архивация таблиц"""
    try:
        print(f"Archive request: tables={tables}, archive_all={archive_all}")
        
        if archive_all:
            # Архивировать все таблицы
            success, result = db.archive_all_tables()
        else:
            # Архивировать выбранные таблицы
            try:
                tables_list = json.loads(tables)
                if not isinstance(tables_list, list):
                    tables_list = []
            except:
                tables_list = []
            
            if not tables_list:
                return {"success": False, "error": "Выберите таблицы для архивации"}
            
            success, result = db.archive_tables(tables_list)
        
        if success:
            return {
                "success": True,
                "message": result["message"],
                "archive_dir": result["archive_dir"],
                "tables_archived": result["tables_archived"],
                "total_tables": result.get("total_tables", 0),
                "details": result.get("details", [])
            }
        else:
            return {"success": False, "error": result}
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Archive error: {str(e)}\n{error_details}")
        return {"success": False, "error": str(e)}

# ==================== ЗАГРУЗКА ФАЙЛОВ ====================
@app.get("/api/service/download/{folder}/{filename}")
async def download_file(folder: str, filename: str):
    """Скачать файл из папки"""
    filepath = Path(folder) / filename
    
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="Файл не найден")
    
    # Определяем Content-Type по расширению
    ext = filepath.suffix.lower()
    if ext == '.backup':
        media_type = "application/octet-stream"
    elif ext == '.xlsx':
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif ext == '.json':
        media_type = "application/json"
    elif ext == '.sql':
        media_type = "text/plain"
    elif ext == '.csv':
        media_type = "text/csv"
    else:
        media_type = "application/octet-stream"
    
    return FileResponse(
        str(filepath),
        media_type=media_type,
        filename=filename
    )

# ==================== ПОЛУЧЕНИЕ СПИСКА ФАЙЛОВ ====================
@app.get("/api/service/backup-files")
async def get_backup_files():
    """Получить список файлов бэкапов"""
    try:
        files = db.get_backup_files()
        return {"success": True, "files": files}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/service/export-files")
async def get_export_files():
    """Получить список файлов экспортов"""
    try:
        files = db.get_export_files()
        return {"success": True, "files": files}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/service/archive-files")
async def get_archive_files():
    """Получить список файлов архивов"""
    try:
        files = db.get_archive_files()
        return {"success": True, "files": files}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    host = os.getenv('APP_HOST', '0.0.0.0')
    port = int(os.getenv('APP_PORT', 3000))
    
    print(f"Сервер запущен на http://localhost:{port}")
    print(f"Доступно по http://127.0.0.1:{port}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True
    )