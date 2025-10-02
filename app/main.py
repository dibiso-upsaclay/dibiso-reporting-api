import subprocess
import tempfile
import shutil
import zipfile
import os
import sys
from pathlib import Path
import logging
import uuid
import asyncio
import threading
import time
import concurrent.futures
import urllib.parse
import requests

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import Optional, Dict, Annotated
# from starlette.requests import Request
from starlette.responses import JSONResponse

# Not needed as used in a subprocess script
# from dibisoreporting import Biso

load_dotenv()  # Load environment variables from .env file (only for development; when in docker, there is no env file
# to load)

scanr_api_password = os.getenv("SCANR_API_PASSWORD")
scanr_api_url = os.getenv("SCANR_API_URL")
scanr_api_username = os.getenv("SCANR_API_USERNAME")
scanr_bso_index = os.getenv("SCANR_BSO_INDEX")
scanr_publications_index = os.getenv("SCANR_PUBLICATIONS_INDEX")
projects_persistence_time_hours = int(os.getenv("PROJECTS_PERSISTENCE_TIME_HOURS"))
latex_main_file_url = os.getenv("LATEX_MAIN_FILE_URL")
latex_biblio_file_url = os.getenv("LATEX_BIBLIO_FILE_URL")
latex_template_url = os.getenv("LATEX_TEMPLATE_URL")
openalex_analysis_cache_path = os.getenv("OPENALEX_ANALYSIS_CACHE_PATH")
openalex_api_key = os.getenv("OPENALEX_API_KEY")
openalex_email = os.getenv("OPENALEX_EMAIL")

# Authentication Imports
from fastapi.security import OAuth2PasswordRequestForm
from .auth import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
    get_current_admin_user,
    ACCESS_TOKEN_EXPIRE_HOURS,
    verify_password,
    Token
)

# User Management Imports
from .users import (
    UserCreate,
    UserResponse,
    UserUpdate,
    PasswordChange,
    AdminPasswordChange,
    create_user_in_db,
    get_user_by_username,
    get_user_by_email,
    get_user_by_id,
    update_user_info,
    update_user_info_by_id,
    update_user_password,
    update_user_password_by_id,
    deactivate_user,
    deactivate_user_by_id,
    activate_user_by_id,
    get_all_users,
    init_database,
    is_admin,
    delete_user_by_id  # Add this import
)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if openalex_analysis_cache_path is not None:
    if not Path(openalex_analysis_cache_path).exists():
        logger.info(f"Creating OpenAlex analysis cache path: {openalex_analysis_cache_path}")
        Path(openalex_analysis_cache_path).mkdir(parents=True, exist_ok=True)
    logger.info(f"Using OpenAlex analysis cache path: {openalex_analysis_cache_path}")
else:
    logger.warning(f"OpenAlex analysis cache path not found: {openalex_analysis_cache_path}")
    logger.warning(f"OpenAlex analysis will use the default cache path: ~/openalex-analysis/data")

# Request model for report generation
class ReportRequest(BaseModel):
    year: int = Field(..., ge=1000, le=3000, description="Year for the report")
    lab_acronym: str = Field(..., min_length=1, description="Laboratory acronym")
    lab_name: str = Field(..., min_length=1, description="Full laboratory name")
    lab_id: str = Field(..., min_length=1, description="HAL collection ID")
    max_entities:int = Field(..., ge=1, le=10000, description="Max entities to use for maps")

thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=int(os.getenv("THREAD_POOL_MAX_WORKERS")))

# Global dictionary to store compilation status
compilation_status: Dict[str, Dict] = {}
compilation_lock = threading.Lock()

# Add these new global variables for process management
data_fetching_processes: Dict[str, subprocess.Popen] = {}
latex_compilation_processes: Dict[str, subprocess.Popen] = {}
process_lock = threading.Lock()


# Cleanup old compilation statuses and files (run periodically)
async def cleanup_old_compilations():
    """
    Clean up compilation statuses older than 1 hour and
    delete temporary directories older than 1 hour based on their modification time.
    """
    while True:
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            current_time = datetime.now()

            # --- Clean up old compilation statuses in memory ---
            with compilation_lock:
                to_remove_status = []
                for comp_id, status in compilation_status.items():
                    if current_time - status['last_updated'] > timedelta(hours=projects_persistence_time_hours):
                        to_remove_status.append(comp_id)

                for comp_id in to_remove_status:
                    del compilation_status[comp_id]

            if to_remove_status:
                logger.info(f"Cleaned up {len(to_remove_status)} old compilation statuses from memory.")

            # --- Clean up old temporary directories on file system ---
            temp_root_dir = Path(tempfile.gettempdir())
            cleaned_up_dirs_count = 0

            # Iterate through all entries in the temporary directory
            for entry in temp_root_dir.iterdir():
                # Check if it's a directory and starts with the expected prefix
                if entry.is_dir() and entry.name.startswith("latex_output_"):
                    try:
                        # Get the last modification time of the directory
                        # This serves as a proxy for creation time for these temporary folders.
                        mod_timestamp = entry.stat().st_mtime
                        mod_datetime = datetime.fromtimestamp(mod_timestamp)

                        # If the directory was modified more than an hour ago, delete it
                        if mod_datetime < current_time - timedelta(hours=projects_persistence_time_hours):
                            logger.info(f"Deleting old temporary directory: {entry}")
                            shutil.rmtree(entry)
                            cleaned_up_dirs_count += 1
                    except Exception as e:
                        logger.error(f"Error cleaning up directory {entry}: {e}")

            if cleaned_up_dirs_count > 0:
                logger.info(
                    f"Cleaned up {cleaned_up_dirs_count} old temporary compilation directories from file system."
                )

        except Exception as e:
            logger.error(f"Error in cleanup task: {e}")


async def monitor_thread_pool():
    """Monitor thread pool health and log statistics"""
    while True:
        try:
            await asyncio.sleep(60)  # Check every minute
            active_tasks = len([t for t in asyncio.all_tasks() if not t.done()])
            logger.info(f"Thread pool status - Active tasks: {active_tasks}")

            # Log compilation statuses
            with compilation_lock:
                running_count = len([s for s in compilation_status.values() if s['status'] == 'running'])
                failed_count = len([s for s in compilation_status.values() if s['status'] == 'failed'])
                completed_count = len([s for s in compilation_status.values() if s['status'] == 'completed'])
            logger.info(f"Compilation status - Running: {running_count}, Failed: {failed_count}, Completed: {completed_count}")

        except Exception as e:
            logger.error(f"Error in thread pool monitoring: {e}")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Context manager for managing the lifespan of the FastAPI application.
    """
    logger.info("Application startup: Starting background tasks and initializing database.")
    init_database()

    # Start background tasks
    cleanup_task = asyncio.create_task(cleanup_old_compilations())
    monitor_task = asyncio.create_task(monitor_thread_pool())

    yield

    logger.info("Application shutdown: Performing cleanup.")

    # Cancel background tasks
    cleanup_task.cancel()
    monitor_task.cancel()

    # Shutdown thread pool
    thread_pool.shutdown(wait=True, timeout=30)

    # Cancel any running compilations
    with compilation_lock:
        for comp_id in list(compilation_status.keys()):
            if compilation_status[comp_id]['status'] == 'running':
                compilation_status[comp_id]['status'] = 'cancelled'

    logger.info("Application shutdown complete.")


app = FastAPI(title=os.getenv("API_TITLE"), version=os.getenv("API_VERSION"), lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "").split(","),
    allow_credentials=os.getenv("CORS_ALLOW_CREDENTIALS", "false").lower() == "true",
    allow_methods=os.getenv("CORS_ALLOW_METHODS", "").split(","),
    allow_headers=os.getenv("CORS_ALLOW_HEADERS", "").split(","),
)


def not_found_error(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=404,
        content={"detail": "Not Found", "doc_url": urllib.parse.urljoin(str(request.base_url), "docs")},
    )


# Register the error handler using the app.exception_handler decorator
@app.exception_handler(404)
def not_found_exception_handler(request: Request, exc: HTTPException):
    return not_found_error(request, exc)


# Authentication endpoints
@app.post("/admin/register", response_model=UserResponse)
async def admin_register_user(
    user_data: UserCreate,
    current_admin: Annotated[dict, Depends(get_current_admin_user)]
):
    """Register a new user (admin only)"""
    # Check if username already exists
    if get_user_by_username(user_data.username):
        raise HTTPException(
            status_code=400,
            detail="Username already registered"
        )

    # Check if email already exists
    if get_user_by_email(user_data.email):
        raise HTTPException(
            status_code=400,
            detail="Email already registered"
        )

    # Create user
    user = create_user_in_db(user_data)

    logger.info(f"Admin {current_admin['username']} created new user: {user_data.username} with role: {user_data.role}")

    return UserResponse(
        id=user["id"],
        username=user["username"],
        email=user["email"],
        role=user["role"],
        is_active=user["is_active"],
        created_at=datetime.fromisoformat(user["created_at"])
    )


@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    """Login endpoint to get access token"""
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/me", response_model=UserResponse)
async def read_users_me(current_user: Annotated[dict, Depends(get_current_active_user)]):
    """Get current user information"""
    return UserResponse(
        id=current_user["id"],
        username=current_user["username"],
        email=current_user["email"],
        role=current_user["role"],
        is_active=current_user["is_active"],
        created_at=datetime.fromisoformat(current_user["created_at"])
    )


@app.put("/users/me", response_model=UserResponse)
async def update_current_user(
    user_update: UserUpdate,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """Update current user information (email only for non-admins)"""
    if user_update.email:
        # Check if email is already taken by another user
        existing_user = get_user_by_email(user_update.email)
        if existing_user and existing_user["username"] != current_user["username"]:
            raise HTTPException(
                status_code=400,
                detail="Email already registered"
            )

        # Only allow role changes for admins changing their own role
        if user_update.role and not is_admin(current_user):
            raise HTTPException(
                status_code=403,
                detail="Only admins can change roles"
            )

        update_user_info(current_user["username"], user_update.email, user_update.role)

    # Get updated user
    updated_user = get_user_by_username(current_user["username"])

    return UserResponse(
        id=updated_user["id"],
        username=updated_user["username"],
        email=updated_user["email"],
        role=updated_user["role"],
        is_active=updated_user["is_active"],
        created_at=datetime.fromisoformat(updated_user["created_at"])
    )


@app.post("/users/me/change-password")
async def change_password(
    password_data: PasswordChange,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """Change current user password"""
    # Verify current password
    if not verify_password(password_data.current_password, current_user["hashed_password"]):
        raise HTTPException(
            status_code=400,
            detail="Incorrect current password"
        )

    # Update password
    update_user_password(current_user["username"], password_data.new_password)

    return {"message": "Password updated successfully"}


@app.delete("/users/me")
async def deactivate_current_user(current_user: Annotated[dict, Depends(get_current_active_user)]):
    """Deactivate current user account"""
    deactivate_user(current_user["username"])
    return {"message": "Account deactivated successfully"}


# Admin endpoints
@app.get("/admin/users", response_model=list[UserResponse])
async def get_all_users_admin(current_admin: Annotated[dict, Depends(get_current_admin_user)]):
    """Get all users (admin only)"""
    users = get_all_users()

    return [
        UserResponse(
            id=user["id"],
            username=user["username"],
            email=user["email"],
            role=user["role"],
            is_active=user["is_active"],
            created_at=datetime.fromisoformat(user["created_at"])
        )
        for user in users
    ]


@app.put("/admin/users/{user_id}", response_model=UserResponse)
async def admin_update_user(
    user_id: int,
    user_update: UserUpdate,
    current_admin: Annotated[dict, Depends(get_current_admin_user)]
):
    """Update user information (admin only)"""
    # Get the user to update
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Check if email is already taken by another user
    if user_update.email:
        existing_user = get_user_by_email(user_update.email)
        if existing_user and existing_user["id"] != user_id:
            raise HTTPException(
                status_code=400,
                detail="Email already registered"
            )

    # Update user
    update_user_info_by_id(user_id, user_update.email, user_update.role)

    # Get updated user
    updated_user = get_user_by_id(user_id)

    logger.info(f"Admin {current_admin['username']} updated user {user['username']}")

    return UserResponse(
        id=updated_user["id"],
        username=updated_user["username"],
        email=updated_user["email"],
        role=updated_user["role"],
        is_active=updated_user["is_active"],
        created_at=datetime.fromisoformat(updated_user["created_at"])
    )


@app.post("/admin/users/{user_id}/change-password")
async def admin_change_user_password(
    user_id: int,
    password_data: AdminPasswordChange,
    current_admin: Annotated[dict, Depends(get_current_admin_user)]
):
    """Change user password (admin only)"""
    # Check if user exists
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Update password
    update_user_password_by_id(user_id, password_data.new_password)

    logger.info(f"Admin {current_admin['username']} changed password for user {user['username']}")

    return {"message": "Password updated successfully"}


@app.post("/admin/users/{user_id}/deactivate")
async def admin_deactivate_user(
    user_id: int,
    current_admin: Annotated[dict, Depends(get_current_admin_user)]
):
    """Deactivate user (admin only)"""
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent admin from deactivating themselves
    if user_id == current_admin["id"]:
        raise HTTPException(
            status_code=400,
            detail="Cannot deactivate your own account"
        )

    deactivate_user_by_id(user_id)

    logger.info(f"Admin {current_admin['username']} deactivated user {user['username']}")

    return {"message": "User deactivated successfully"}


@app.post("/admin/users/{user_id}/activate")
async def admin_activate_user(
    user_id: int,
    current_admin: Annotated[dict, Depends(get_current_admin_user)]
):
    """Activate user (admin only)"""
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    activate_user_by_id(user_id)

    logger.info(f"Admin {current_admin['username']} activated user {user['username']}")

    return {"message": "User activated successfully"}


@app.delete("/admin/users/{user_id}")
async def admin_delete_user(
    user_id: int,
    current_admin: Annotated[dict, Depends(get_current_admin_user)]
):
    """Permanently delete user (admin only)"""
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent admin from deleting themselves
    if user_id == current_admin["id"]:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete your own account"
        )

    # Check if this is the last active admin
    if user["role"] == "admin" and user["is_active"]:
        # Count active admins
        all_users = get_all_users()
        active_admin_count = sum(1 for u in all_users if u["role"] == "admin" and u["is_active"] and u["id"] != user_id)
        
        if active_admin_count == 0:
            raise HTTPException(
                status_code=400,
                detail="Cannot delete the last active admin user"
            )

    success = delete_user_by_id(user_id)
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete user")

    logger.info(f"Admin {current_admin['username']} deleted user {user['username']} (ID: {user_id})")

    return {"message": "User deleted successfully"}


# Report generation endpoints
def update_compilation_status(comp_id: str, progress: int, step: str, status: str = "running"):
    """Update the compilation status for a given compilation ID"""
    with compilation_lock:
        if comp_id in compilation_status:
            compilation_status[comp_id].update({
                'progress': progress,
                'current_step': step,
                'status': status,
                'last_updated': datetime.now()
            })


async def update_compilation_status_async(comp_id: str, progress: int, step: str, status: str = "running"):
    """Async version of update_compilation_status"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, update_compilation_status, comp_id, progress, step, status)


def run_latex_compile_command(comp_id, project_folder, tex_file, pass_i, process_args):
    # Start the latex command as a subprocess that can be killed
    process = subprocess.Popen(
        process_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=project_folder
    )

    # Store the process reference for cancellation
    with process_lock:
        latex_compilation_processes[comp_id] = process

    try:
        # Wait for completion while checking for cancellation
        while process.poll() is None:  # While process is still running
            time.sleep(0.25)  # Check every 250ms
            with compilation_lock:
                if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                    # Kill the lualatex process
                    try:
                        process.terminate()
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                        logger.info(f"Terminated lualatex process for {comp_id}")
                    except Exception as e:
                        logger.error(f"Error terminating lualatex process: {e}")
                    return

        # Get the result
        stdout, stderr = process.communicate()
        result_returncode = process.returncode

    finally:
        # Remove process reference
        with process_lock:
            latex_compilation_processes.pop(comp_id, None)
        update_compilation_status(
            comp_id,
            0,
            f"LaTeX compilation of {tex_file} failed ({process_args}) (pass {pass_i})",
            "failed"
        )

    if result_returncode != 0:
        logger.error(f"LaTeX compilation of {tex_file} failed (pass {pass_i}):")
        logger.error(stdout)
        logger.error(stderr)
        # Only update status to failed if it's not already cancelled
        # with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
            update_compilation_status(
                comp_id,
                0,
                f"LaTeX compilation of {tex_file} failed (pass {pass_i})",
                "failed"
            )
        raise RuntimeError(f"LaTeX compilation of {tex_file} failed (pass {pass_i})")


def launch_latex_compile_command(comp_id, progress, project_folder, tex_file, pass_i, total_pass, command = "latex"):
    # Check for cancellation before each pass
    with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
            return

    if command == "biber":
        step_name = f"Running Biber for {tex_file}..."
        process_args = [
            'biber',
            '-output-directory', str(project_folder),
            str(Path(tex_file).stem)
        ]
    else:
        step_name = f"Running LuaTeX on {tex_file} (pass {pass_i}/{total_pass})..."
        process_args = [
            'lualatex',
            '-interaction=nonstopmode',
            '-output-directory', str(project_folder),
            str(tex_file)
        ]
    update_compilation_status(comp_id, progress, step_name)

    run_latex_compile_command(
        comp_id = latex_main_file_url,
        project_folder = project_folder,
        tex_file = tex_file,
        pass_i = pass_i,
        process_args = process_args
    )


def compile_latex_with_progress(project_folder: Path, comp_id: str) -> list[Optional[Path]]:
    """
    Compile LaTeX project in the given folder with progress updates and cancellation support.
    Returns path to generated PDF or None if compilation failed or was cancelled.
    """
    # Check for cancellation
    with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
            return [None, None]

    main_tex_file = Path(os.path.basename(latex_main_file_url))
    biblio_tex_file = Path(os.path.basename(latex_biblio_file_url))

    try:
        # Run LuaTeX on main tex file twice to resolve references
        launch_latex_compile_command(comp_id, 73, project_folder, main_tex_file, 1, 3)
        launch_latex_compile_command(comp_id, 76, project_folder, main_tex_file, 2, 3)
        launch_latex_compile_command(comp_id, 79, project_folder, main_tex_file, 3, 3)
        launch_latex_compile_command(comp_id, 82, project_folder, biblio_tex_file, 1, 4)
        launch_latex_compile_command(comp_id, 85, project_folder, biblio_tex_file, 1, 1, "biber")
        launch_latex_compile_command(comp_id, 88, project_folder, biblio_tex_file, 2, 4)
        launch_latex_compile_command(comp_id, 91, project_folder, biblio_tex_file, 3, 4)
        launch_latex_compile_command(comp_id, 94, project_folder, biblio_tex_file, 4, 4)

        # Final check for cancellation
        with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                return [None, None]

        update_compilation_status(comp_id, 97, "Finalizing PDF generation...")

        # Check if PDF was generated
        pdf_names = [main_tex_file.stem + '.pdf', biblio_tex_file.stem + '.pdf']
        pdf_paths = []
        for pdf_name in pdf_names:
            pdf_path = project_folder / pdf_name

            if pdf_path.exists():
                update_compilation_status(comp_id, 98, "PDF generated successfully!")
                pdf_paths.append(pdf_path)
            else:
                logger.error("PDF file was not generated")
                # Only update status to failed if it's not already cancelled
                # with compilation_lock:
                if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
                    update_compilation_status(comp_id, 0, "Error: PDF file was not generated", "failed")
                pdf_paths.append(None)
        return pdf_paths

    except FileNotFoundError:
        logger.error("lualatex command not found. Please install LaTeX distribution.")
        # Only update status to failed if it's not already cancelled
        # with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
            update_compilation_status(comp_id, 0, "Error: lualatex not found", "failed")
        return [None, None]
    except Exception as e:
        # if the error was not previously handled
        if not("LaTeX compilation of" in str(e) and "failed (pass" in str(e)):
            logger.error(f"Unexpected error during compilation: {e}")
            # Only update status to failed if it's not already cancelled
            # with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
                update_compilation_status(comp_id, 0, f"Error: {str(e)}", "failed")
        return [None, None]


def create_zip_archive(project_folder: Path, zip_path: Path) -> bool:
    """
    Create a ZIP archive of the project folder.
    Returns True if successful, False otherwise.
    """
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in project_folder.rglob('*'):
                if file_path.is_file():
                    # Add file to zip with relative path
                    arcname = file_path.relative_to(project_folder)
                    zipf.write(file_path, arcname)
        return True
    except Exception as e:
        logger.error(f"Failed to create ZIP archive: {e}")
        return False


def your_latex_project_generator(comp_id: str, request_data: ReportRequest) -> Optional[Path]:
    """
    Generate LaTeX project with improved error handling and cancellation support.
    """
    # Check if cancelled before starting
    with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
            return None
    update_compilation_status(comp_id, 10, "Fetching data and making plots (this step may take a while...)")
    # Create temporary directory for the project
    project_dir = Path(tempfile.mkdtemp(prefix="latex_project_"))
    try:
        logger.info("Starting subprocess for data fetching with parameters:")
        logger.info(f"  Lab ID: {request_data.lab_id}")
        logger.info(f"  Year: {request_data.year}")
        logger.info(f"  Lab Acronym: {request_data.lab_acronym}")
        logger.info(f"  Lab Name: {request_data.lab_name}")
        logger.info(f"  Max entities: {request_data.max_entities}")

        # Start the data fetching + report generation as a subprocess with dynamic parameters
        process = subprocess.Popen([
            sys.executable, '-c',
            f'''
import sys
sys.path.insert(0, "{os.getcwd()}")

from openalex_analysis.data import config as openalex_analysis_config
from dibisoreporting import Biso

if "{str(openalex_analysis_cache_path)}" != None:
    openalex_analysis_config.project_data_folder_path = "{str(openalex_analysis_cache_path)}"
if "{str(openalex_api_key)}" != None:
    openalex_analysis_config.api_key = "{str(openalex_api_key)}"
if "{str(openalex_email)}" != None:
    openalex_analysis_config.email = "{str(openalex_email)}"


biso_reporting = Biso(
    "{request_data.lab_id}",
    {request_data.year},
    lab_acronym="{request_data.lab_acronym}",
    lab_full_name="{request_data.lab_name}",
    latex_main_file_url="{latex_main_file_url}",
    latex_biblio_file_url="{latex_biblio_file_url}",
    latex_template_url="{latex_template_url}",
    max_entities={request_data.max_entities},
    root_path="{project_dir}",
    watermark_text="",
    scanr_api_password="{scanr_api_password}",
    scanr_api_url="{scanr_api_url}",
    scanr_api_username="{scanr_api_username}",
    scanr_bso_index="{scanr_bso_index}",
    scanr_publications_index="{scanr_publications_index}",
)
biso_reporting.generate_report()
            '''
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # Store the process reference for cancellation
        with process_lock:
            data_fetching_processes[comp_id] = process

        # Wait for completion with timeout and cancellation checks
        start_time = time.time()
        timeout_seconds = 1800  # 30 minutes
        while process.poll() is None:
            # Check for timeout
            if time.time() - start_time > timeout_seconds:
                logger.error(f"Process timeout after {timeout_seconds} seconds for {comp_id}")
                try:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                except Exception as e:
                    logger.error(f"Error terminating timed out process: {e}")
                # Clean up and return early
                if project_dir.exists():
                    shutil.rmtree(project_dir)
                # with compilation_lock:
                if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
                    update_compilation_status(comp_id, 0, "Process timeout - data fetching took too long", "failed")
                return None

            # Check for cancellation
            with compilation_lock:
                if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                    try:
                        process.terminate()
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                        logger.info(f"Terminated data fetching process for {comp_id}")
                    except Exception as e:
                        logger.error(f"Error terminating data fetching process: {e}")
                    # Clean up and return early
                    if project_dir.exists():
                        shutil.rmtree(project_dir)
                    return None
            time.sleep(0.5)  # Check every 500ms

        # Get results
        try:
            stdout, stderr = process.communicate(timeout=10)  # 10 second timeout for communication
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout waiting for process communication for {comp_id}")
            process.kill()
            stdout, stderr = process.communicate()

        logger.info("Subprocess stdout: " + stdout)
        if stderr:
            logger.error("Subprocess stderr: " + stderr)

        # Remove process reference immediately
        with process_lock:
            data_fetching_processes.pop(comp_id, None)

        # Check return code and handle failure immediately
        if process.returncode != 0:
            logger.error(f"Data fetching failed with return code {process.returncode}")
            logger.info(f"Cleaning up the generated project")
            if project_dir.exists():
                try:
                    shutil.rmtree(project_dir)
                except Exception as e:
                    logger.error(f"Error cleaning up project dir: {e}")
            logger.info("Update status to failed")
            # with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
                error_msg = "Data fetching failed"
                if "JSONDecodeError" in stderr:
                    error_msg = ("Data fetching failed due to a JSON decode error. "
                                 "This might occur if the server response is empty or malformed. "
                                 "Please check the HAL collection ID or contact your admin.")
                elif "ConnectionError" in stderr:
                    error_msg = "Data fetching failed - network connection error"
                elif "TimeoutError" in stderr:
                    error_msg = "Data fetching failed - request timeout"
                logger.info(error_msg)
                update_compilation_status(comp_id, 0, error_msg, "failed")
            logger.info("Return, don't continue processing")
            return None

        # Final cancellation check before returning success
        with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                if project_dir.exists():
                    shutil.rmtree(project_dir)
                return None
        logger.info(f"Data fetching completed successfully for {comp_id}")
        return project_dir
    except subprocess.TimeoutExpired:
        logger.error(f"Subprocess timed out for {comp_id}")
        if project_dir.exists():
            shutil.rmtree(project_dir)
        # with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
            update_compilation_status(comp_id, 0, "Data fetching timeout", "failed")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in project generation for {comp_id}: {e}")
        if project_dir.exists():
            try:
                shutil.rmtree(project_dir)
            except Exception as cleanup_error:
                logger.error(f"Error during cleanup: {cleanup_error}")
        # with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
            update_compilation_status(comp_id, 0, f"Generation error: {str(e)}", "failed")
        return None



def run_compilation(comp_id: str, request_data: ReportRequest):
    """Run the compilation with improved error handling and early exits"""
    try:
        # Initial status update
        update_compilation_status(comp_id, 2, "Checking HAL collection ID...")

        # check that the HAL collection ID is valid
        url = f"https://api.archives-ouvertes.fr/search/?q=collCode_s:{request_data.lab_id}&wt=json&rows=0"
        coll_exists = requests.get(url).json().get('response',{}).get('numFound', 0) > 0
        if not coll_exists:
            logging.info(f"Collection ID {request_data.lab_id} does not exist in HAL. Aborting report generation.")
            update_compilation_status(comp_id, 0, "The HAL collection ID doesn't exist", "failed")
            return

        update_compilation_status(comp_id, 5, "Starting generation process...")

        # Check for cancellation immediately
        with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                logger.info(f"Compilation {comp_id} cancelled during initialization.")
                return

        # Generate the LaTeX project (this is the long-running part)
        logger.info(f"Generating LaTeX project for {comp_id}...")
        project_folder = your_latex_project_generator(comp_id, request_data)

        # CRITICAL: Exit immediately if project generation failed or was cancelled
        if project_folder is None:
            logger.info(f"Project generation failed or was cancelled for {comp_id}, exiting compilation")
            return  # Exit immediately, status already updated in generator function

        # Check if compilation was cancelled after project generation
        with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                logger.info(f"Compilation {comp_id} cancelled after project generation.")
                if project_folder.exists():
                    try:
                        shutil.rmtree(project_folder)
                    except Exception as e:
                        logger.error(f"Error cleaning up project folder: {e}")
                return

        # Create temporary directory for outputs
        temp_dir = Path(tempfile.mkdtemp(prefix="latex_output_"))

        # Compile the LaTeX project
        logger.info(f"Compiling LaTeX project in {project_folder} for {comp_id}")
        pdf_paths = compile_latex_with_progress(project_folder, comp_id)

        if None in pdf_paths:
            logger.info(f"LaTeX compilation failed or was cancelled for {comp_id}")
            # Clean up
            cleanup_directories(project_folder, temp_dir)
            update_compilation_status(
                comp_id,
                0,
                f"LaTeX compilation failed",
                "failed"
            )
            return

        # Check for cancellation before creating ZIP
        with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                logger.info(f"Compilation {comp_id} cancelled before ZIP creation.")
                cleanup_directories(project_folder, temp_dir)
                return

        update_compilation_status(comp_id, 98, "Creating ZIP archive...")

        # Create ZIP archive
        zip_path = temp_dir / "project.zip"
        logger.info(f"Creating ZIP archive at {zip_path} for {comp_id}")

        if not create_zip_archive(project_folder, zip_path):
            # with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
                update_compilation_status(comp_id, 0, "Failed to create ZIP archive", "failed")
            cleanup_directories(project_folder, temp_dir)
            return

        # Copy PDF to temp directory
        report_pdf = temp_dir / "report.pdf"
        biblio_pdf = temp_dir / "biblio.pdf"
        try:
            shutil.copy2(pdf_paths[0], report_pdf)
            shutil.copy2(pdf_paths[1], biblio_pdf)
        except Exception as e:
            logger.error(f"Error copying PDF for {comp_id}: {e}")
            # with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
                update_compilation_status(comp_id, 0, "Failed to prepare output files", "failed")
            cleanup_directories(project_folder, temp_dir)
            return

        # Clean up the original project folder
        try:
            shutil.rmtree(project_folder)
        except Exception as e:
            logger.error(f"Error cleaning up project folder for {comp_id}: {e}")

        # Final check for cancellation before marking as completed
        with compilation_lock:
            if compilation_status.get(comp_id, {}).get('status') == 'cancelled':
                logger.info(f"Compilation {comp_id} cancelled before final status update.")
                if temp_dir.exists():
                    try:
                        shutil.rmtree(temp_dir)
                    except Exception as e:
                        logger.error(f"Error cleaning up temp dir: {e}")
                return

            # Mark as completed only if still running
            if compilation_status.get(comp_id, {}).get('status') == 'running':
                compilation_status[comp_id].update({
                    'progress': 100,
                    'current_step': 'Compilation completed successfully!',
                    'status': 'completed',
                    'result': {
                        'message': f'LaTeX report generated successfully for {request_data.lab_acronym} '
                                   f'({request_data.year})',
                        'pdf_url': '/download-pdf',
                        'zip_url': '/download-zip',
                        'temp_id': temp_dir.name
                    },
                    'temp_dir': str(temp_dir),
                    'last_updated': datetime.now()
                })
            else:
                logger.warning(
                    f"Compilation {comp_id} not marked as completed because status is "
                    f"{compilation_status[comp_id]['status']}"
                )

        logger.info(f"LaTeX compilation completed successfully for {comp_id}")

    except Exception as e:
        logger.error(f"Critical error in background compilation for {comp_id}: {e}")
        # with compilation_lock:
        if compilation_status.get(comp_id, {}).get('status') != 'cancelled':
            update_compilation_status(comp_id, 0, f"Compilation error: {str(e)}", "failed")


def cleanup_directories(*dirs):
    """Helper function to safely clean up directories"""
    for directory in dirs:
        if directory and Path(directory).exists():
            try:
                shutil.rmtree(directory)
            except Exception as e:
                logger.error(f"Error cleaning up directory {directory}: {e}")


def verify_and_get_file_path(temp_id: str, current_user: dict, filename: str) -> Path:
    """
    Verify that the current user owns the file associated with temp_id and return the file path.
    Returns the file path if valid and exists, raises HTTPException otherwise.
    """
    # Find the compilation that generated this file and verify ownership
    compilation_owner = None
    with compilation_lock:
        for comp_id, status in compilation_status.items():
            if status.get('temp_dir') == str(Path(tempfile.gettempdir()) / temp_id):
                if status.get('user_id') == current_user["id"]:
                    compilation_owner = current_user["id"]
                break
    
    if not compilation_owner:
        raise HTTPException(
            status_code=403, 
            detail="Access denied - file not found or you don't own this file"
        )
    
    # Additional security: ensure temp_id doesn't contain path traversal attempts
    if not temp_id.replace('_', '').replace('-', '').isalnum() or len(temp_id) < 10:
        raise HTTPException(status_code=400, detail="Invalid temporary ID format")
    
    temp_dir = Path(tempfile.gettempdir()) / temp_id
    file_path = temp_dir / filename
    
    if not file_path.exists():
        file_type = filename.split('.')[-1].upper()
        raise HTTPException(status_code=404, detail=f"{file_type} file not found")
    
    return file_path


async def run_compilation_async(comp_id: str, request_data: ReportRequest):
    """Run the compilation process in the background without blocking the event loop"""
    loop = asyncio.get_event_loop()
    # The actual blocking work is done inside run_compilation, which is run in the thread_pool
    await loop.run_in_executor(thread_pool, run_compilation, comp_id, request_data)


@app.post("/generate-report")
async def generate_report_endpoint(
    request: ReportRequest,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """
    Start LaTeX compilation with the provided parameters and return compilation ID for progress tracking.
    Requires authentication.
    """
    # Validate the request (Pydantic will handle basic validation)
    logger.info(f"Received report generation request from user {current_user['username']}: {request.dict()}")

    # Generate unique compilation ID
    comp_id = str(uuid.uuid4())

    # Initialize compilation status
    with compilation_lock:
        compilation_status[comp_id] = {
            'progress': 0,
            'current_step': 'Initializing...',
            'status': 'running',
            'request_data': request.dict(),  # Store request data for reference
            'user_id': current_user["id"],  # Store user who initiated the request
            'username': current_user["username"],
            'created_at': datetime.now(),
            'last_updated': datetime.now()
        }

    # Start background compilation as a task instead of using BackgroundTasks
    asyncio.create_task(run_compilation_async(comp_id, request))

    return {
        "message": f"Generation started for {request.lab_acronym} ({request.year})",
        "compilation_id": comp_id,
        "parameters": {
            "year": request.year,
            "lab_acronym": request.lab_acronym,
            "lab_name": request.lab_name,
            "lab_id": request.lab_id
        }
    }


@app.get("/compilation-status/{comp_id}")
async def get_compilation_status(
    comp_id: str,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """Get the current status of a compilation. Users can only see their own compilations."""
    with compilation_lock:
        if comp_id not in compilation_status:
            raise HTTPException(status_code=404, detail="Compilation ID not found")

        status = compilation_status[comp_id]

        # Check if user owns this compilation
        if status.get('user_id') != current_user["id"]:
            raise HTTPException(status_code=403, detail="Access denied to this compilation")

        status_copy = status.copy()

    # Remove internal fields from response
    status_copy.pop('temp_dir', None)
    status_copy.pop('result', None)
    status_copy.pop('created_at', None)
    status_copy.pop('last_updated', None)
    status_copy.pop('request_data', None)
    status_copy.pop('user_id', None)  # Remove user_id from response

    return status_copy


@app.get("/compilation-result/{comp_id}")
async def get_compilation_result(
    comp_id: str,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """Get the result of a completed compilation. Users can only see their own compilations."""
    with compilation_lock:
        if comp_id not in compilation_status:
            raise HTTPException(status_code=404, detail="Compilation ID not found")

        status = compilation_status[comp_id]

        # Check if user owns this compilation
        if status.get('user_id') != current_user["id"]:
            raise HTTPException(status_code=403, detail="Access denied to this compilation")

        if status['status'] != 'completed':
            raise HTTPException(status_code=400, detail="Compilation not completed yet")

        if 'result' not in status:
            raise HTTPException(status_code=500, detail="Compilation result not available")

    return status['result']


@app.post("/cancel-compilation/{comp_id}")
async def cancel_compilation(
    comp_id: str,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """Cancel a running compilation. Users can only cancel their own compilations."""
    with compilation_lock:
        if comp_id not in compilation_status:
            raise HTTPException(status_code=404, detail="Compilation ID not found")

        status = compilation_status[comp_id]

        # Check if user owns this compilation
        if status.get('user_id') != current_user["id"]:
            raise HTTPException(status_code=403, detail="Access denied to this compilation")

        if status['status'] not in ['running', 'initializing']:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel compilation with status: {status['status']}"
            )

        # Update status to cancelled
        compilation_status[comp_id].update({
            'status': 'cancelled',
            'current_step': 'Compilation cancelled by user',
            'progress': 0,
            'last_updated': datetime.now()
        })

        # Clean up temporary directory if it exists
        temp_dir_str = status.get('temp_dir')
        if temp_dir_str:
            temp_dir = Path(temp_dir_str)
            try:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                    logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up temp directory {temp_dir}: {e}")

    # Kill any running processes
    with process_lock:
        # Kill data fetching process if running
        if comp_id in data_fetching_processes:
            process = data_fetching_processes[comp_id]
            try:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                logger.info(f"Successfully terminated data fetching process for {comp_id}")
            except Exception as e:
                logger.error(f"Error terminating data fetching process for {comp_id}: {e}")

        # Kill LaTeX compilation process if running
        if comp_id in latex_compilation_processes:
            process = latex_compilation_processes[comp_id]
            try:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                logger.info(f"Successfully terminated LaTeX compilation process for {comp_id}")
            except Exception as e:
                logger.error(f"Error terminating LaTeX compilation process for {comp_id}: {e}")

    return {"message": "Compilation cancelled successfully"}


@app.get("/download-pdf")
async def download_pdf(
    temp_id: str,
    file_name: str,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """Download the generated PDF file. Requires authentication."""
    pdf_path = verify_and_get_file_path(temp_id, current_user, file_name + ".pdf")
    
    return FileResponse(
        path=pdf_path,
        filename=file_name,
        media_type="application/pdf"
    )


@app.get("/download-zip")
async def download_zip(
    temp_id: str,
    current_user: Annotated[dict, Depends(get_current_active_user)]
):
    """Download the project ZIP archive. Requires authentication."""
    zip_path = verify_and_get_file_path(temp_id, current_user, "project.zip")
    
    return FileResponse(
        path=zip_path,
        filename="latex_project.zip",
        media_type="application/zip"
    )


# Health check endpoint (public)
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "message": "BiSO generator API is running"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("DEV_API_HOST"), port=int(os.getenv("DEV_API_PORT")))
