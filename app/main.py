from fastapi import FastAPI, Request, Form, Depends, HTTPException, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import os
import json
from datetime import datetime
import tempfile
from pathlib import Path
import asyncio

import data_manager as dm
db_handler = dm.DatabaseManager()

app = FastAPI(
    title="Database Management Interface",
    description="Web-based database administration tool",
    version="2.0.0",
    docs_url=None,
    redoc_url=None
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Create required directories
def setup_directories():
    directories = ["static", "templates", "backups", "exports", "archives"]
    for dir_name in directories:
        Path(dir_name).mkdir(exist_ok=True)
        print(f"Directory ensured: {dir_name}")

setup_directories()

# Setup static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# =============== UTILITY FUNCTIONS ===============
def get_current_timestamp() -> str:
    """Get current timestamp in ISO format"""
    return datetime.now().isoformat()

def format_response(success: bool, data: Any = None, message: str = "", error: str = "") -> Dict:
    """Standardized response format"""
    return {
        "status": "success" if success else "error",
        "timestamp": get_current_timestamp(),
        "data": data,
        "message": message,
        "error": error
    }

# =============== ROUTE HANDLERS ===============
@app.get("/", response_class=HTMLResponse)
async def dashboard_view(request: Request):
    """Main dashboard view"""
    try:
        table_list = db_handler.get_table_names()
        table_stats = {}
        
        for table in table_list:
            table_stats[table] = db_handler.count_table_rows(table)
        
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "tables": table_list,
            "table_stats": table_stats,
            "total_tables": len(table_list)
        })
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Database connection error: {str(e)}"
        })

@app.get("/tables", response_class=HTMLResponse)
async def tables_management(request: Request, table: str = "", page: int = 1):
    """Table data management interface"""
    table_list = db_handler.get_table_names() or []
    columns_info = []
    table_data = []
    total_rows = 0
    page_count = 0
    rows_per_page = 100
    
    if table and table in table_list:
        columns_info = db_handler.get_table_structure(table) or []
        total_rows = db_handler.count_table_rows(table)
        page_count = (total_rows + rows_per_page - 1) // rows_per_page
        
        page = max(1, min(page, page_count))
        offset = (page - 1) * rows_per_page
        
        table_data = db_handler.fetch_table_data(table, limit=rows_per_page, offset=offset) or []
    
    return templates.TemplateResponse("tables.html", {
        "request": request,
        "all_tables": table_list,
        "selected_table": table,
        "columns": columns_info,
        "data": table_data,
        "current_page": page,
        "rows_per_page": rows_per_page,
        "total_rows": total_rows,
        "page_count": page_count
    })

@app.get("/query", response_class=HTMLResponse)
async def query_interface(request: Request):
    """SQL query interface"""
    table_list = db_handler.get_table_names() or []
    return templates.TemplateResponse("query.html", {
        "request": request,
        "tables": table_list
    })

@app.get("/tools", response_class=HTMLResponse)
async def tools_panel(request: Request):
    """Database tools and utilities"""
    table_list = db_handler.get_table_names() or []
    return templates.TemplateResponse("tools.html", {
        "request": request,
        "tables": table_list
    })

@app.get("/api/status")
async def api_status():
    """API status endpoint"""
    return format_response(
        success=True,
        message="Database Management Interface is operational",
        data={
            "version": "2.0.0",
            "timestamp": get_current_timestamp(),
            "database": "Connected" if db_handler.get_table_names() else "Disconnected"
        }
    )

@app.get("/api/tables")
async def api_get_tables():
    """Get list of all tables"""
    try:
        tables = db_handler.get_table_names()
        return format_response(
            success=True,
            data={"tables": tables},
            message=f"Found {len(tables)} tables"
        )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Failed to retrieve tables: {str(e)}"
        )

@app.get("/api/tables/{table_name}/structure")
async def api_get_table_structure(table_name: str):
    """Get table structure"""
    try:
        structure = db_handler.get_table_structure(table_name)
        return format_response(
            success=True,
            data={"structure": structure},
            message=f"Structure for table '{table_name}'"
        )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Failed to get table structure: {str(e)}"
        )

@app.post("/api/tables/{table_name}/data")
async def api_insert_data(
    table_name: str,
    row_data: str = Form(...)
):
    """Insert data into table"""
    try:
        data_dict = json.loads(row_data)
        inserted_id = db_handler.add_table_row(table_name, data_dict)
        
        if inserted_id:
            return format_response(
                success=True,
                data={"inserted_id": inserted_id},
                message=f"Row added to '{table_name}' with ID: {inserted_id}"
            )
        else:
            return format_response(
                success=False,
                error=f"Failed to insert data into '{table_name}'"
            )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Data insertion error: {str(e)}"
        )

@app.put("/api/tables/{table_name}/data")
async def api_update_data(
    table_name: str,
    update_data: str = Form(...),
    filter_condition: str = Form(...)
):
    """Update table data"""
    try:
        data_dict = json.loads(update_data)
        cleaned_data = {k: v for k, v in data_dict.items() if v is not None and v != ''}
        
        if not cleaned_data:
            return format_response(
                success=False,
                error="No data provided for update"
            )
        
        result = db_handler.modify_table_data(table_name, cleaned_data, filter_condition)
        
        if result:
            return format_response(
                success=True,
                message=f"Data updated in '{table_name}' with cascade processing"
            )
        elif result is False:
            return format_response(
                success=False,
                error=f"No rows found matching condition in '{table_name}'"
            )
        else:
            return format_response(
                success=False,
                error=f"Update operation failed for '{table_name}'"
            )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Update error: {str(e)}"
        )

@app.delete("/api/tables/{table_name}/data")
async def api_delete_data(
    table_name: str,
    filter_condition: str = Form(...),
    cascade_mode: bool = Form(False)
):
    """Delete data from table"""
    try:
        if not filter_condition or filter_condition.strip() == "":
            return format_response(
                success=False,
                error="Deletion condition cannot be empty"
            )
        
        if cascade_mode:
            result = db_handler.remove_table_data(table_name, filter_condition)
            if result:
                return format_response(
                    success=True,
                    message=f"Data removed from '{table_name}' with cascade deletion"
                )
            else:
                return format_response(
                    success=False,
                    error=f"Cascade deletion failed for '{table_name}'"
                )
        else:
            result = db_handler.safe_remove_table_data(table_name, filter_condition)
            
            if isinstance(result, dict):
                if result.get('status'):
                    return format_response(
                        success=True,
                        data={"affected_rows": result.get('rows_affected', 0)},
                        message=f"Removed {result.get('rows_affected', 0)} rows from '{table_name}'"
                    )
                else:
                    if result.get('error_type') == 'dependencies_exist':
                        dependencies = result.get('dependency_list', [])
                        dep_details = "\n".join([f"- {d['table']}: {d['count']} rows" for d in dependencies])
                        
                        return format_response(
                            success=False,
                            data={
                                "has_dependencies": True,
                                "dependencies": dependencies
                            },
                            error=f"Cannot delete due to existing dependencies:\n{dep_details}\n\nUse cascade mode to delete all related data."
                        )
                    else:
                        return format_response(
                            success=False,
                            error=result.get('error_message', 'Unknown deletion error')
                        )
            else:
                return format_response(
                    success=False,
                    error=f"Deletion operation failed for '{table_name}'"
                )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Deletion error: {str(e)}"
        )

@app.delete("/api/tables/{table_name}")
async def api_delete_table(table_name: str):
    """Delete entire table"""
    try:
        result = db_handler.drop_database_table(table_name)
        if result:
            return format_response(
                success=True,
                message=f"Table '{table_name}' has been removed"
            )
        else:
            return format_response(
                success=False,
                error=f"Failed to remove table '{table_name}'"
            )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Table deletion error: {str(e)}"
        )

@app.post("/api/query/execute")
async def api_execute_query(
    query_text: str = Form(...),
    query_params: str = Form("")
):
    """Execute SQL query"""
    try:
        params_dict = {}
        if query_params and query_params.strip():
            try:
                params_dict = json.loads(query_params)
            except json.JSONDecodeError as e:
                return format_response(
                    success=False,
                    error=f"Invalid parameters format: {str(e)}"
                )
        
        query_result = db_handler.run_sql_query(query_text, params_dict)
        
        return format_response(
            success=True,
            data={
                "result": query_result,
                "row_count": len(query_result) if query_result else 0
            },
            message="Query executed successfully"
        )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Query execution error: {str(e)}"
        )

@app.post("/api/export/query")
async def api_export_query_result(
    query_text: str = Form(...),
    query_params: str = Form(""),
    export_format: str = Form("excel")
):
    """Export query results"""
    try:
        params_dict = {}
        if query_params and query_params.strip():
            try:
                params_dict = json.loads(query_params)
            except json.JSONDecodeError as e:
                return format_response(
                    success=False,
                    error=f"Invalid parameters format: {str(e)}"
                )
        
        query_result = db_handler.run_sql_query(query_text, params_dict)
        
        if not query_result:
            return format_response(
                success=False,
                error="No data available for export"
            )
        
        if export_format == "excel":
            filepath, error = db_handler.export_query_to_spreadsheet(query_result)
            
            if filepath:
                filename = f"query_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                return FileResponse(
                    filepath,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=filename
                )
            else:
                return format_response(
                    success=False,
                    error=error or "Failed to create export file"
                )
        
        elif export_format == "json":
            json_data = json.dumps(query_result, ensure_ascii=False, indent=2, default=str)
            filename = f"query_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            return JSONResponse({
                "status": "success",
                "filename": filename,
                "content": json_data,
                "format": "json"
            })
        
        else:
            return format_response(
                success=False,
                error=f"Unsupported export format: {export_format}"
            )
            
    except Exception as e:
        return format_response(
            success=False,
            error=f"Export error: {str(e)}"
        )

@app.post("/api/backup/create")
async def api_create_backup():
    """Create database backup"""
    try:
        backup_status, backup_path, error_message = db_handler.create_database_backup()
        
        if backup_status:
            return format_response(
                success=True,
                data={"backup_path": backup_path},
                message=f"Database backup created: {backup_path}"
            )
        else:
            return format_response(
                success=False,
                error=error_message
            )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Backup creation error: {str(e)}"
        )

@app.post("/api/backup/restore")
async def api_restore_backup(backup_file: UploadFile = File(...)):
    """Restore database from backup"""
    try:
        if not backup_file.filename:
            return format_response(
                success=False,
                error="No backup file selected"
            )
        
        if not backup_file.filename.lower().endswith('.backup'):
            return format_response(
                success=False,
                error="Only PostgreSQL .backup files are supported"
            )
        
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, backup_file.filename)
        
        with open(temp_file_path, 'wb') as f:
            file_content = await backup_file.read()
            f.write(file_content)
        
        restore_status, restore_message = db_handler.restore_database_backup(temp_file_path)
        
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        if restore_status:
            return format_response(
                success=True,
                message=restore_message + " - Refreshing interface in 3 seconds..."
            )
        else:
            if "unrecognized configuration parameter \"transaction_timeout\"" in restore_message:
                return format_response(
                    success=True,
                    message="Restoration completed with warnings ignored - Refreshing interface in 3 seconds..."
                )
            return format_response(
                success=False,
                error=restore_message
            )
    except Exception as e:
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        return format_response(
            success=False,
            error=f"Restoration error: {str(e)}"
        )

@app.post("/api/archive/tables")
async def api_archive_tables(
    tables_to_archive: str = Form("[]"),
    archive_all_flag: bool = Form(False)
):
    """Archive selected tables"""
    try:
        if archive_all_flag:
            archive_status, archive_result = db_handler.archive_all_database_tables()
        else:
            try:
                tables_list = json.loads(tables_to_archive)
                if not isinstance(tables_list, list):
                    tables_list = []
            except:
                tables_list = []
            
            if not tables_list:
                return format_response(
                    success=False,
                    error="Select tables for archiving"
                )
            
            archive_status, archive_result = db_handler.archive_database_tables(tables_list)
        
        if archive_status:
            return format_response(
                success=True,
                data={
                    "archive_directory": archive_result.get("archive_dir"),
                    "tables_processed": archive_result.get("tables_archived"),
                    "total_tables": archive_result.get("total_tables", 0),
                    "details": archive_result.get("details", [])
                },
                message=archive_result.get("message", "Archiving completed")
            )
        else:
            return format_response(
                success=False,
                error=archive_result
            )
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Archive processing error: {str(e)}\n{error_details}")
        return format_response(
            success=False,
            error=f"Archiving error: {str(e)}"
        )

@app.get("/api/files/backups")
async def api_get_backup_files():
    """Get list of backup files"""
    try:
        backup_files = db_handler.list_backup_files()
        return format_response(
            success=True,
            data={"files": backup_files}
        )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Failed to list backup files: {str(e)}"
        )

@app.get("/api/files/exports")
async def api_get_export_files():
    """Get list of export files"""
    try:
        export_files = db_handler.list_export_files()
        return format_response(
            success=True,
            data={"files": export_files}
        )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Failed to list export files: {str(e)}"
        )

@app.get("/api/files/archives")
async def api_get_archive_files():
    """Get list of archive files"""
    try:
        archive_files = db_handler.list_archive_files()
        return format_response(
            success=True,
            data={"files": archive_files}
        )
    except Exception as e:
        return format_response(
            success=False,
            error=f"Failed to list archive files: {str(e)}"
        )

@app.get("/api/download/{category}/{filename}")
async def api_download_file(category: str, filename: str):
    """Download file from server"""
    filepath = Path(category) / filename
    
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")
    
    extension_map = {
        '.backup': 'application/octet-stream',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.json': 'application/json',
        '.sql': 'text/plain',
        '.csv': 'text/csv'
    }
    
    file_extension = filepath.suffix.lower()
    media_type = extension_map.get(file_extension, 'application/octet-stream')
    
    return FileResponse(
        str(filepath),
        media_type=media_type,
        filename=filename
    )

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "database-management-interface",
        "timestamp": get_current_timestamp(),
        "version": "2.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    server_host = os.getenv('APP_HOST', '0.0.0.0')
    server_port = int(os.getenv('APP_PORT', 3000))
    
    print(f"Database Management Interface")
    print(f"Server: http://{server_host}:{server_port}")
    print(f"API Documentation: http://{server_host}:{server_port}/docs")
    print(f"Health Check: http://{server_host}:{server_port}/health")
    
    uvicorn.run(
        app,
        host=server_host,
        port=server_port,
        log_level="info"
    )