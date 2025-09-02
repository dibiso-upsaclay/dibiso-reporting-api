import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict
from pydantic import BaseModel, EmailStr

# Database directory and file name
USERS_DATABASE_NAME = os.getenv("USERS_DATABASE_NAME")
USERS_DATABASE_DIRECTORY = os.getenv("USERS_DATABASE_DIRECTORY")
DATABASE_PATH=os.path.join(USERS_DATABASE_DIRECTORY, USERS_DATABASE_NAME)

# Pydantic models
class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    role: Optional[str] = "user"  # Default to user role

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    role: str
    is_active: bool
    created_at: datetime

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    role: Optional[str] = None

class PasswordChange(BaseModel):
    current_password: str
    new_password: str

class AdminPasswordChange(BaseModel):
    user_id: int
    new_password: str

def init_database():
    """Initialize the SQLite database"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            hashed_password TEXT NOT NULL,
            role TEXT DEFAULT 'user' CHECK(role IN ('user', 'admin')),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()

    # Create initial admin user if none exists
    create_initial_admin()

def create_initial_admin():
    """Create the initial admin user from environment variables if no admin exists"""
    # Check if any admin user already exists
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as count FROM users WHERE role = 'admin' AND is_active = TRUE")
    result = cursor.fetchone()
    admin_count = result["count"] if result else 0
    conn.close()

    if admin_count == 0:
        # No admin exists, create one from environment variables
        admin_username = os.getenv("ADMIN_USERNAME")
        admin_password = os.getenv("ADMIN_PASSWORD")

        if not admin_username or not admin_password:
            print("WARNING: No admin user found and ADMIN_USERNAME/ADMIN_PASSWORD not set in environment")
            return

        # Create admin user
        try:
            from .auth import get_password_hash
            hashed_password = get_password_hash(admin_password)

            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()

            # Use a default email for admin if not specified
            admin_email = os.getenv("ADMIN_EMAIL", f"{admin_username}@localhost")

            cursor.execute('''
                INSERT INTO users (username, email, hashed_password, role)
                VALUES (?, ?, ?, 'admin')
            ''', (admin_username, admin_email, hashed_password))

            conn.commit()
            conn.close()
            print(f"Initial admin user '{admin_username}' created successfully")
        except Exception as e:
            print(f"Error creating initial admin user: {e}")

def get_user_by_username(username: str) -> Optional[Dict]:
    """Get user by username"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None

def get_user_by_id(user_id: int) -> Optional[Dict]:
    """Get user by ID"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None

def get_user_by_email(email: EmailStr) -> Optional[Dict]:
    """Get user by email"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE email = ?", (email,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None

def create_user_in_db(user_data: UserCreate) -> Dict:
    """Create a new user in the database"""
    from .auth import get_password_hash

    hashed_password = get_password_hash(user_data.password)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO users (username, email, hashed_password, role)
        VALUES (?, ?, ?, ?)
    ''', (user_data.username, user_data.email, hashed_password, user_data.role))

    user_id = cursor.lastrowid
    conn.commit()

    # Fetch the created user
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = dict(cursor.fetchone())
    conn.close()

    return user

def update_user_info(username: str, email: EmailStr | None = None, role: str = None):
    """Update user information"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    if email and role:
        cursor.execute('''
            UPDATE users SET email = ?, role = ? WHERE username = ?
        ''', (email, role, username))
    elif email:
        cursor.execute('''
            UPDATE users SET email = ? WHERE username = ?
        ''', (email, username))
    elif role:
        cursor.execute('''
            UPDATE users SET role = ? WHERE username = ?
        ''', (role, username))

    conn.commit()
    conn.close()

def update_user_info_by_id(user_id: int, email: EmailStr | None = None, role: str = None):
    """Update user information by ID"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    if email and role:
        cursor.execute('''
            UPDATE users SET email = ?, role = ? WHERE id = ?
        ''', (email, role, user_id))
    elif email:
        cursor.execute('''
            UPDATE users SET email = ? WHERE id = ?
        ''', (email, user_id))
    elif role:
        cursor.execute('''
            UPDATE users SET role = ? WHERE id = ?
        ''', (role, user_id))

    conn.commit()
    conn.close()

def update_user_password(username: str, new_password: str):
    """Update user password"""
    from .auth import get_password_hash

    hashed_password = get_password_hash(new_password)

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE users SET hashed_password = ? WHERE username = ?
    ''', (hashed_password, username))

    conn.commit()
    conn.close()

def update_user_password_by_id(user_id: int, new_password: str):
    """Update user password by ID (for admin use)"""
    from .auth import get_password_hash

    hashed_password = get_password_hash(new_password)

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE users SET hashed_password = ? WHERE id = ?
    ''', (hashed_password, user_id))

    conn.commit()
    conn.close()

def deactivate_user(username: str):
    """Deactivate a user"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE users SET is_active = FALSE WHERE username = ?
    ''', (username,))

    conn.commit()
    conn.close()

def deactivate_user_by_id(user_id: int):
    """Deactivate a user by ID"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE users SET is_active = FALSE WHERE id = ?
    ''', (user_id,))

    conn.commit()
    conn.close()

def activate_user_by_id(user_id: int):
    """Activate a user by ID"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE users SET is_active = TRUE WHERE id = ?
    ''', (user_id,))

    conn.commit()
    conn.close()

def get_all_users() -> List[Dict]:
    """Get all users"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users ORDER BY created_at DESC")
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]

def is_admin(user: Dict) -> bool:
    """Check if user has admin role"""
    return user.get("role") == "admin"
