use sqlx::{SqlitePool, Row};
use uuid::Uuid;
use chrono::Utc;
use crate::models::*;
use gurtlib::Result;

pub async fn get_database_pool() -> Result<SqlitePool> {
    let db_path = std::env::current_dir()
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get current directory: {}", e)))?
        .join("gurtpay.db");
    
    let connection_string = format!("sqlite:{}", db_path.display());
    let pool = SqlitePool::connect(&connection_string).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Database connection failed: {}", e)))?;
    
    Ok(pool)
}

pub async fn init_database() -> Result<SqlitePool> {
    // Ensure the database file can be created by using an absolute path
    let db_path = std::env::current_dir()
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get current directory: {}", e)))?
        .join("gurtpay.db");
    
    println!("📁 Database path: {}", db_path.display());
    
    // Create the database file if it doesn't exist
    if !db_path.exists() {
        std::fs::File::create(&db_path)
            .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create database file: {}", e)))?;
        println!("📄 Created new database file");
    }
    
    let pool = get_database_pool().await?;
    
    // Create tables
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            arsonflare_id TEXT UNIQUE NOT NULL,
            username TEXT NOT NULL,
            wallet_balance REAL DEFAULT 5000.0,
            wallet_address TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL,
            is_admin BOOLEAN DEFAULT FALSE
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create users table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS businesses (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            business_name TEXT NOT NULL,
            website_url TEXT,
            api_key TEXT UNIQUE NOT NULL,
            verified BOOLEAN DEFAULT TRUE,
            balance REAL DEFAULT 0.0,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create businesses table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            transaction_type TEXT NOT NULL,
            from_user_id TEXT,
            to_user_id TEXT,
            business_id TEXT,
            amount REAL NOT NULL,
            platform_fee REAL DEFAULT 0.0,
            status TEXT DEFAULT 'completed',
            description TEXT NOT NULL,
            created_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY (from_user_id) REFERENCES users (id),
            FOREIGN KEY (to_user_id) REFERENCES users (id),
            FOREIGN KEY (business_id) REFERENCES businesses (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create transactions table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS redemption_codes (
            id TEXT PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            amount REAL NOT NULL,
            max_uses INTEGER,
            current_uses INTEGER DEFAULT 0,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            active BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (created_by) REFERENCES users (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create redemption_codes table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS code_redemptions (
            id TEXT PRIMARY KEY,
            code_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            amount_received REAL NOT NULL,
            redeemed_at TEXT NOT NULL,
            FOREIGN KEY (code_id) REFERENCES redemption_codes (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE(code_id, user_id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create code_redemptions table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS invoices (
            id TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT NOT NULL,
            customer_name TEXT,
            status TEXT DEFAULT 'pending',
            paid_at TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            FOREIGN KEY (business_id) REFERENCES businesses (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create invoices table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS user_sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            jwt_token TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            active BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create user_sessions table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS money_requests (
            id TEXT PRIMARY KEY,
            from_user_id TEXT NOT NULL,
            to_user_id TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT NOT NULL,
            responded_at TEXT,
            FOREIGN KEY (from_user_id) REFERENCES users (id),
            FOREIGN KEY (to_user_id) REFERENCES users (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create money_requests table: {}", e)))?;

    Ok(pool)
}

pub async fn create_user(pool: &SqlitePool, arsonflare_id: &str, username: &str) -> Result<User> {
    let id = Uuid::new_v4();
    let wallet_address = generate_wallet_address();
    let created_at = Utc::now();
    
    sqlx::query(
        "INSERT INTO users (id, arsonflare_id, username, wallet_address, created_at) 
         VALUES (?, ?, ?, ?, ?)"
    )
    .bind(id.to_string())
    .bind(arsonflare_id)
    .bind(username)
    .bind(&wallet_address)
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create user: {}", e)))?;
    
    // Create welcome transaction
    let transaction_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO transactions (id, transaction_type, to_user_id, amount, status, description, created_at, completed_at)
         VALUES (?, 'welcome', ?, 5000.0, 'completed', 'Welcome to GurtPay!', ?, ?)"
    )
    .bind(transaction_id.to_string())
    .bind(id.to_string())
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create welcome transaction: {}", e)))?;
    
    Ok(User {
        id,
        arsonflare_id: arsonflare_id.to_string(),
        username: username.to_string(),
        wallet_balance: 5000.0,
        wallet_address,
        created_at,
        is_admin: false,
    })
}

pub async fn get_user_by_arsonflare_id(pool: &SqlitePool, arsonflare_id: &str) -> Result<Option<User>> {
    let row = sqlx::query(
        "SELECT id, arsonflare_id, username, wallet_balance, wallet_address, created_at, is_admin 
         FROM users WHERE arsonflare_id = ?"
    )
    .bind(arsonflare_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Database query failed: {}", e)))?;
    
    match row {
        Some(row) => {
            Ok(Some(User {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                arsonflare_id: row.get("arsonflare_id"),
                username: row.get("username"),
                wallet_balance: row.get("wallet_balance"),
                wallet_address: row.get("wallet_address"),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
                is_admin: row.get("is_admin"),
            }))
        }
        None => Ok(None),
    }
}

pub async fn get_user_by_wallet_address(pool: &SqlitePool, wallet_address: &str) -> Result<Option<User>> {
    let row = sqlx::query(
        "SELECT id, arsonflare_id, username, wallet_balance, wallet_address, created_at, is_admin 
         FROM users WHERE wallet_address = ?"
    )
    .bind(wallet_address)
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Database query failed: {}", e)))?;
    
    match row {
        Some(row) => {
            Ok(Some(User {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                arsonflare_id: row.get("arsonflare_id"),
                username: row.get("username"),
                wallet_balance: row.get("wallet_balance"),
                wallet_address: row.get("wallet_address"),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
                is_admin: row.get("is_admin"),
            }))
        }
        None => Ok(None),
    }
}

pub async fn get_user_by_id(pool: &SqlitePool, user_id: Uuid) -> Result<Option<User>> {
    let row = sqlx::query(
        "SELECT id, arsonflare_id, username, wallet_balance, wallet_address, created_at, is_admin 
         FROM users WHERE id = ?"
    )
    .bind(user_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Database query failed: {}", e)))?;
    
    match row {
        Some(row) => {
            Ok(Some(User {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                arsonflare_id: row.get("arsonflare_id"),
                username: row.get("username"),
                wallet_balance: row.get("wallet_balance"),
                wallet_address: row.get("wallet_address"),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
                is_admin: row.get("is_admin"),
            }))
        }
        None => Ok(None),
    }
}

pub async fn transfer_funds(pool: &SqlitePool, from_user_id: &Uuid, to_user_id: &Uuid, amount: f64, description: &str) -> Result<Transaction> {
    let mut tx = pool.begin().await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to start transaction: {}", e)))?;
    
    // Check sender balance
    let sender_balance: f64 = sqlx::query_scalar("SELECT wallet_balance FROM users WHERE id = ?")
        .bind(from_user_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get sender balance: {}", e)))?;
    
    if sender_balance < amount {
        return Err(gurtlib::GurtError::invalid_message("Insufficient funds".to_string()));
    }
    
    // Update balances
    sqlx::query("UPDATE users SET wallet_balance = wallet_balance - ? WHERE id = ?")
        .bind(amount)
        .bind(from_user_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to debit sender: {}", e)))?;
    
    sqlx::query("UPDATE users SET wallet_balance = wallet_balance + ? WHERE id = ?")
        .bind(amount)
        .bind(to_user_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to credit recipient: {}", e)))?;
    
    // Create transaction record
    let transaction_id = Uuid::new_v4();
    let created_at = Utc::now();
    
    sqlx::query(
        "INSERT INTO transactions (id, transaction_type, from_user_id, to_user_id, amount, status, description, created_at, completed_at)
         VALUES (?, 'transfer', ?, ?, ?, 'completed', ?, ?, ?)"
    )
    .bind(transaction_id.to_string())
    .bind(from_user_id.to_string())
    .bind(to_user_id.to_string())
    .bind(amount)
    .bind(description)
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .execute(&mut *tx)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create transaction: {}", e)))?;
    
    tx.commit().await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to commit transaction: {}", e)))?;
    
    Ok(Transaction {
        id: transaction_id,
        transaction_type: TransactionType::Transfer,
        from_user_id: Some(*from_user_id),
        to_user_id: Some(*to_user_id),
        business_id: None,
        amount,
        platform_fee: 0.0,
        status: TransactionStatus::Completed,
        description: description.to_string(),
        created_at,
        completed_at: Some(created_at),
    })
}

pub fn generate_wallet_address() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let chars: String = (0..8)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .collect();
    format!("GC{}", chars.to_uppercase())
}

// Invoice functions
pub async fn create_invoice(
    pool: &SqlitePool,
    business_id: Uuid,
    amount: f64,
    description: &str,
    customer_name: Option<&str>,
    expires_at: Option<chrono::DateTime<Utc>>,
) -> Result<Invoice> {
    let invoice_id = Uuid::new_v4();
    let now = Utc::now();
    
    sqlx::query(r#"
        INSERT INTO invoices (id, business_id, amount, description, customer_name, status, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
    "#)
    .bind(invoice_id.to_string())
    .bind(business_id.to_string())
    .bind(amount)
    .bind(description)
    .bind(customer_name)
    .bind(now.to_rfc3339())
    .bind(expires_at.map(|t| t.to_rfc3339()))
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create invoice: {}", e)))?;
    
    Ok(Invoice {
        id: invoice_id,
        business_id,
        amount,
        description: description.to_string(),
        customer_name: customer_name.map(|s| s.to_string()),
        status: InvoiceStatus::Pending,
        paid_at: None,
        created_at: now,
        expires_at,
    })
}

pub async fn get_invoice(pool: &SqlitePool, invoice_id: Uuid) -> Result<Option<Invoice>> {
    let row = sqlx::query(r#"
        SELECT id, business_id, amount, description, customer_name, status, paid_at, created_at, expires_at
        FROM invoices WHERE id = ?
    "#)
    .bind(invoice_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get invoice: {}", e)))?;
    
    match row {
        Some(row) => {
            let status = match row.get::<String, _>("status").as_str() {
                "pending" => InvoiceStatus::Pending,
                "paid" => InvoiceStatus::Paid,
                "expired" => InvoiceStatus::Expired,
                "cancelled" => InvoiceStatus::Cancelled,
                _ => InvoiceStatus::Pending,
            };
            
            Ok(Some(Invoice {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                business_id: Uuid::parse_str(&row.get::<String, _>("business_id")).unwrap(),
                amount: row.get("amount"),
                description: row.get("description"),
                customer_name: row.get("customer_name"),
                status,
                paid_at: row.get::<Option<String>, _>("paid_at").map(|s| chrono::DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
                expires_at: row.get::<Option<String>, _>("expires_at").map(|s| chrono::DateTime::parse_from_rfc3339(&s).unwrap().with_timezone(&Utc)),
            }))
        }
        None => Ok(None),
    }
}

pub async fn mark_invoice_paid(pool: &SqlitePool, invoice_id: Uuid) -> Result<()> {
    let now = Utc::now();
    
    sqlx::query(r#"
        UPDATE invoices SET status = 'paid', paid_at = ? WHERE id = ?
    "#)
    .bind(now.to_rfc3339())
    .bind(invoice_id.to_string())
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to mark invoice paid: {}", e)))?;
    
    Ok(())
}

pub async fn get_business_by_api_key(pool: &SqlitePool, api_key: &str) -> Result<Option<Business>> {
    let row = sqlx::query(r#"
        SELECT id, user_id, business_name, website_url, api_key, verified, balance, created_at
        FROM businesses WHERE api_key = ?
    "#)
    .bind(api_key)
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get business by API key: {}", e)))?;
    
    match row {
        Some(row) => {
            Ok(Some(Business {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                user_id: Uuid::parse_str(&row.get::<String, _>("user_id")).unwrap(),
                business_name: row.get("business_name"),
                website_url: row.get("website_url"),
                api_key: row.get("api_key"),
                verified: row.get("verified"),
                balance: row.get("balance"),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
            }))
        }
        None => Ok(None),
    }
}

pub async fn get_business_by_id(pool: &SqlitePool, business_id: Uuid) -> Result<Option<Business>> {
    let row = sqlx::query(r#"
        SELECT id, user_id, business_name, website_url, api_key, verified, balance, created_at
        FROM businesses WHERE id = ?
    "#)
    .bind(business_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get business by id: {}", e)))?;
    
    match row {
        Some(row) => {
            Ok(Some(Business {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                user_id: Uuid::parse_str(&row.get::<String, _>("user_id")).unwrap(),
                business_name: row.get("business_name"),
                website_url: row.get("website_url"),
                api_key: row.get("api_key"),
                verified: row.get("verified"),
                balance: row.get("balance"),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
            }))
        }
        None => Ok(None),
    }
}

pub async fn transfer_to_business(
    pool: &SqlitePool,
    from_user_id: &Uuid,
    business_id: &Uuid,
    amount: f64,
    description: &str,
) -> Result<Transaction> {
    let mut tx = pool.begin().await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to start transaction: {}", e)))?;
    
    let user_balance: f64 = sqlx::query_scalar("SELECT wallet_balance FROM users WHERE id = ?")
        .bind(from_user_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get user balance: {}", e)))?;
    
    if user_balance < amount {
        return Err(gurtlib::GurtError::invalid_message("Insufficient funds".to_string()));
    }
    
    sqlx::query("UPDATE users SET wallet_balance = wallet_balance - ? WHERE id = ?")
        .bind(amount)
        .bind(from_user_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to debit user: {}", e)))?;
    
    sqlx::query("UPDATE businesses SET balance = balance + ? WHERE id = ?")
        .bind(amount)
        .bind(business_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to credit business: {}", e)))?;
    
    let transaction_id = Uuid::new_v4();
    let created_at = Utc::now();
    sqlx::query(
        "INSERT INTO transactions (id, transaction_type, from_user_id, to_user_id, business_id, amount, platform_fee, status, description, created_at, completed_at) 
         VALUES (?, 'business_payment', ?, NULL, ?, ?, 0.0, 'completed', ?, ?, ?)"
    )
    .bind(transaction_id.to_string())
    .bind(from_user_id.to_string())
    .bind(business_id.to_string())
    .bind(amount)
    .bind(description)
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .execute(&mut *tx)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create transaction: {}", e)))?;
    
    tx.commit().await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to commit transaction: {}", e)))?;
    
    Ok(Transaction {
        id: transaction_id,
        transaction_type: TransactionType::BusinessPayment,
        from_user_id: Some(*from_user_id),
        to_user_id: None,
        business_id: Some(*business_id),
        amount,
        platform_fee: 0.0,
        status: TransactionStatus::Completed,
        description: description.to_string(),
        created_at,
        completed_at: Some(created_at),
    })
}

pub async fn get_user_session(pool: &SqlitePool, session_token: &str) -> Result<Option<crate::models::UserSession>> {
    let row = sqlx::query(r#"
        SELECT id, user_id, session_token, created_at, expires_at
        FROM user_sessions WHERE session_token = ? AND expires_at > datetime('now')
    "#)
    .bind(session_token)
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get user session: {}", e)))?;
    
    match row {
        Some(row) => {
            Ok(Some(crate::models::UserSession {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                user_id: Uuid::parse_str(&row.get::<String, _>("user_id")).unwrap(),
                session_token: row.get("session_token"),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
                expires_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("expires_at")).unwrap().with_timezone(&Utc),
            }))
        }
        None => Ok(None),
    }
}
