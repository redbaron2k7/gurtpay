use sqlx::{AnyPool, Row};
use uuid::Uuid;
use chrono::{Utc, Datelike};
use crate::models::*;
use gurtlib::Result;

pub async fn get_database_pool() -> Result<AnyPool> {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_PATH"))
        .unwrap_or_else(|_| "".to_string());

    let conn_string = if db_url.is_empty() {
        let db_path = std::env::current_dir()
            .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get current directory: {}", e)))?
            .join("gurtpay.db");
        format!("sqlite:{}", db_path.display())
    } else if db_url.starts_with("postgres://") || db_url.starts_with("postgresql://") {
        db_url
    } else if db_url.starts_with("sqlite:") || db_url.ends_with(".db") {
        if db_url.starts_with("sqlite:") { db_url } else { format!("sqlite:{}", db_url) }
    } else {
        format!("sqlite:{}", db_url)
    };

    let pool = AnyPool::connect(&conn_string).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Database connection failed: {}", e)))?;
    Ok(pool)
}

pub async fn init_database() -> Result<AnyPool> {
    if std::env::var("DATABASE_URL").is_err() && std::env::var("DATABASE_PATH").is_err() {
        let db_path = std::env::current_dir()
            .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get current directory: {}", e)))?
            .join("gurtpay.db");
        println!("📁 Database path: {}", db_path.display());
        if !db_path.exists() {
            std::fs::File::create(&db_path)
                .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create database file: {}", e)))?;
            println!("📄 Created new database file");
        }
    }

    let pool = get_database_pool().await?;
    
    // Create tables
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            arsonflare_id TEXT UNIQUE NOT NULL,
            username TEXT NOT NULL,
            wallet_balance DOUBLE PRECISION DEFAULT 0.0,
            wallet_address TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL,
            is_admin BOOLEAN DEFAULT FALSE
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create users table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS user_credentials (
            user_id TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create user_credentials table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS businesses (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            business_name TEXT NOT NULL,
            website_url TEXT,
            api_key TEXT UNIQUE NOT NULL,
            verified BOOLEAN DEFAULT TRUE,
            balance DOUBLE PRECISION DEFAULT 0.0,
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
            amount DOUBLE PRECISION NOT NULL,
            platform_fee DOUBLE PRECISION DEFAULT 0.0,
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
            amount DOUBLE PRECISION NOT NULL,
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
            amount_received DOUBLE PRECISION NOT NULL,
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
            amount DOUBLE PRECISION NOT NULL,
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
            amount DOUBLE PRECISION NOT NULL,
            description TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT NOT NULL,
            responded_at TEXT,
            FOREIGN KEY (from_user_id) REFERENCES users (id),
            FOREIGN KEY (to_user_id) REFERENCES users (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create money_requests table: {}", e)))?;

    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS debit_cards (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            card_number TEXT NOT NULL UNIQUE,
            cvv TEXT NOT NULL,
            expiration_month INTEGER NOT NULL,
            expiration_year INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    "#).execute(&pool).await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create debit_cards table: {}", e)))?;

    Ok(pool)
}

pub async fn create_user(pool: &AnyPool, arsonflare_id: &str, username: &str) -> Result<User> {
    let id = Uuid::new_v4();
    let wallet_address = generate_wallet_address();
    let created_at = Utc::now();
    
    sqlx::query(
        "INSERT INTO users (id, arsonflare_id, username, wallet_balance, wallet_address, created_at) 
         VALUES (?, ?, ?, 0.0, ?, ?)"
    )
    .bind(id.to_string())
    .bind(arsonflare_id)
    .bind(username)
    .bind(&wallet_address)
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create user: {}", e)))?;
    
    // Create welcome transaction and credit balance via ledger + wallet_balance
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
    
    sqlx::query("UPDATE users SET wallet_balance = wallet_balance + 5000.0 WHERE id = $1")
        .bind(id.to_string())
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to credit welcome balance: {}", e)))?;
    
    Ok(User {
        id,
        arsonflare_id: arsonflare_id.to_string(),
        username: username.to_string(),
        wallet_balance: 0.0,
        wallet_address,
        created_at,
        is_admin: false,
    })
}

pub async fn get_user_by_arsonflare_id(pool: &AnyPool, arsonflare_id: &str) -> Result<Option<User>> {
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

pub async fn get_user_by_wallet_address(pool: &AnyPool, wallet_address: &str) -> Result<Option<User>> {
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

pub async fn get_user_by_id(pool: &AnyPool, user_id: Uuid) -> Result<Option<User>> {
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

pub async fn transfer_funds(pool: &AnyPool, from_user_id: &Uuid, to_user_id: &Uuid, amount: f64, description: &str) -> Result<Transaction> {
    let mut tx = pool.begin().await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to start transaction: {}", e)))?;
    
    // Check sender balance
    let sender_balance: f64 = sqlx::query_scalar("SELECT wallet_balance FROM users WHERE id = $1")
        .bind(from_user_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get sender balance: {}", e)))?;
    
    if sender_balance < amount {
        return Err(gurtlib::GurtError::invalid_message("Insufficient funds".to_string()));
    }
    
    // Update balances
    sqlx::query("UPDATE users SET wallet_balance = wallet_balance - $1 WHERE id = $2")
        .bind(amount)
        .bind(from_user_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to debit sender: {}", e)))?;
    
    sqlx::query("UPDATE users SET wallet_balance = wallet_balance + $1 WHERE id = $2")
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

pub fn generate_card_number() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    
    // Generate a 16-digit card number starting with 4
    let mut digits = vec![4];
    for _ in 1..16 {
        digits.push(rng.gen_range(0..10));
    }
    
    // Format as XXXX-XXXX-XXXX-XXXX
    format!("{}{}{}{}-{}{}{}{}-{}{}{}{}-{}{}{}{}",
        digits[0], digits[1], digits[2], digits[3],
        digits[4], digits[5], digits[6], digits[7],
        digits[8], digits[9], digits[10], digits[11],
        digits[12], digits[13], digits[14], digits[15])
}

pub fn generate_cvv() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    format!("{:03}", rng.gen_range(100..1000))
}

pub fn generate_expiration() -> (i32, i32) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let current_year = Utc::now().year();
    let future_year = current_year + rng.gen_range(10..20);
    let month = rng.gen_range(1..13);
    (month, future_year)
}

// Invoice functions
pub async fn create_invoice(
    pool: &AnyPool,
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

pub async fn get_invoice(pool: &AnyPool, invoice_id: Uuid) -> Result<Option<Invoice>> {
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

pub async fn mark_invoice_paid(pool: &AnyPool, invoice_id: Uuid) -> Result<()> {
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

pub async fn get_business_by_api_key(pool: &AnyPool, api_key: &str) -> Result<Option<Business>> {
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

pub async fn get_business_by_id(pool: &AnyPool, business_id: Uuid) -> Result<Option<Business>> {
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
    pool: &AnyPool,
    from_user_id: &Uuid,
    business_id: &Uuid,
    amount: f64,
    description: &str,
) -> Result<Transaction> {
    let mut tx = pool.begin().await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to start transaction: {}", e)))?;
    
    let user_balance: f64 = sqlx::query_scalar("SELECT wallet_balance FROM users WHERE id = $1")
        .bind(from_user_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get user balance: {}", e)))?;
    
    if user_balance < amount {
        return Err(gurtlib::GurtError::invalid_message("Insufficient funds".to_string()));
    }
    
    sqlx::query("UPDATE users SET wallet_balance = wallet_balance - $1 WHERE id = $2")
        .bind(amount)
        .bind(from_user_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to debit user: {}", e)))?;
    
    sqlx::query("UPDATE businesses SET balance = balance + $1 WHERE id = $2")
        .bind(amount)
        .bind(business_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to credit business: {}", e)))?;
    
    let transaction_id = Uuid::new_v4();
    let created_at = Utc::now();
    sqlx::query(
        "INSERT INTO transactions (id, transaction_type, from_user_id, to_user_id, business_id, amount, platform_fee, status, description, created_at, completed_at) \
         VALUES ($1, 'business_payment', $2, NULL, $3, $4, 0.0, 'completed', $5, $6, $7)"
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

pub async fn get_user_session(pool: &AnyPool, session_token: &str) -> Result<Option<crate::models::UserSession>> {
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

pub async fn get_user_by_username(pool: &AnyPool, username: &str) -> Result<Option<User>> {
    let row = sqlx::query(
        "SELECT id, arsonflare_id, username, wallet_balance, wallet_address, created_at, is_admin \
         FROM users WHERE LOWER(username) = LOWER($1) OR LOWER(arsonflare_id) = LOWER($2)"
    )
    .bind(username)
    .bind(username)
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Database query failed: {}", e)))?;
    match row {
        Some(row) => Ok(Some(User {
            id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
            arsonflare_id: row.get("arsonflare_id"),
            username: row.get("username"),
            wallet_balance: row.get("wallet_balance"),
            wallet_address: row.get("wallet_address"),
            created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
            is_admin: row.get("is_admin"),
        })),
        None => Ok(None),
    }
}

pub async fn set_user_password_hash(pool: &AnyPool, user_id: &Uuid, password_hash: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO user_credentials (user_id, password_hash) VALUES ($1, $2)\n         ON CONFLICT(user_id) DO UPDATE SET password_hash = excluded.password_hash"
    )
    .bind(user_id.to_string())
    .bind(password_hash)
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to set credentials: {}", e)))?;
    Ok(())
}

pub async fn get_password_hash(pool: &AnyPool, user_id: &Uuid) -> Result<Option<String>> {
    let row = sqlx::query("SELECT password_hash FROM user_credentials WHERE user_id = $1")
        .bind(user_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get credentials: {}", e)))?;
    Ok(row.map(|r| r.get::<String, _>("password_hash")))
}

pub async fn create_user_with_password(pool: &AnyPool, username: &str, password_hash: &str) -> Result<User> {
    let id = Uuid::new_v4();
    let wallet_address = generate_wallet_address();
    let created_at = Utc::now();

    sqlx::query(
        "INSERT INTO users (id, arsonflare_id, username, wallet_balance, wallet_address, created_at) \
         VALUES ($1, $2, $3, 0.0, $4, $5)"
    )
    .bind(id.to_string())
    .bind(username)
    .bind(username)
    .bind(&wallet_address)
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create user: {}", e)))?;

    set_user_password_hash(pool, &id, password_hash).await?;

    // Welcome transaction and credit
    let transaction_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO transactions (id, transaction_type, to_user_id, amount, status, description, created_at, completed_at) \
         VALUES ($1, 'welcome', $2, 5000.0, 'completed', 'Welcome to GurtPay!', $3, $4)"
    )
    .bind(transaction_id.to_string())
    .bind(id.to_string())
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create welcome transaction: {}", e)))?;

    sqlx::query("UPDATE users SET wallet_balance = wallet_balance + 5000.0 WHERE id = $1")
        .bind(id.to_string())
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to credit welcome balance: {}", e)))?;

    Ok(User {
        id,
        arsonflare_id: username.to_string(),
        username: username.to_string(),
        wallet_balance: 0.0,
        wallet_address,
        created_at,
        is_admin: false,
    })
}

// Debit card functions
pub async fn create_debit_card(pool: &AnyPool, user_id: Uuid) -> Result<serde_json::Value> {
    // Check if user already has an active card
    let existing_card_count = sqlx::query("SELECT COUNT(*) as count FROM debit_cards WHERE user_id = $1 AND is_active = TRUE")
        .bind(user_id.to_string())
        .fetch_one(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to check existing cards: {}", e)))?;
    
    let count: i64 = existing_card_count.get("count");
    if count > 0 {
        return Err(gurtlib::GurtError::invalid_message("User already has an active debit card. Use regenerate instead.".to_string()));
    }
    
    let card_id = Uuid::new_v4();
    let card_number = generate_card_number();
    let cvv = generate_cvv();
    let (exp_month, exp_year) = generate_expiration();
    let created_at = Utc::now();

    sqlx::query("INSERT INTO debit_cards (id, user_id, card_number, cvv, expiration_month, expiration_year, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)")
        .bind(card_id.to_string())
        .bind(user_id.to_string())
        .bind(&card_number)
        .bind(&cvv)
        .bind(exp_month)
        .bind(exp_year)
        .bind(created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create debit card: {}", e)))?;

    Ok(serde_json::json!({
        "card_id": card_id,
        "card_number": card_number,
        "cvv": cvv,
        "expiration_month": exp_month,
        "expiration_year": exp_year,
        "is_active": true,
        "created_at": created_at
    }))
}

pub async fn get_user_debit_cards(pool: &AnyPool, user_id: Uuid) -> Result<Vec<serde_json::Value>> {
    let rows = sqlx::query("SELECT id, card_number, cvv, expiration_month, expiration_year, is_active, created_at FROM debit_cards WHERE user_id = $1 AND is_active = TRUE ORDER BY created_at DESC")
        .bind(user_id.to_string())
        .fetch_all(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to get debit cards: {}", e)))?;

    let mut cards = Vec::new();
    for row in rows {
        let card_number: String = row.get("card_number");
        
        cards.push(serde_json::json!({
            "card_id": row.get::<String, _>("id"),
            "card_number": card_number,
            "cvv": row.get::<String, _>("cvv"),
            "expiration_month": row.get::<i32, _>("expiration_month"),
            "expiration_year": row.get::<i32, _>("expiration_year"),
            "is_active": row.get::<bool, _>("is_active"),
            "created_at": row.get::<String, _>("created_at")
        }));
    }

    Ok(cards)
}

pub async fn regenerate_debit_card(pool: &AnyPool, user_id: Uuid) -> Result<serde_json::Value> {
    // First, deactivate the existing card
    sqlx::query("UPDATE debit_cards SET is_active = FALSE WHERE user_id = $1 AND is_active = TRUE")
        .bind(user_id.to_string())
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to deactivate existing card: {}", e)))?;
    
    // Create new card with new details
    let card_id = Uuid::new_v4();
    let card_number = generate_card_number();
    let cvv = generate_cvv();
    let (exp_month, exp_year) = generate_expiration();
    let created_at = Utc::now();

    sqlx::query("INSERT INTO debit_cards (id, user_id, card_number, cvv, expiration_month, expiration_year, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)")
        .bind(card_id.to_string())
        .bind(user_id.to_string())
        .bind(&card_number)
        .bind(&cvv)
        .bind(exp_month)
        .bind(exp_year)
        .bind(created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to create new debit card: {}", e)))?;

    Ok(serde_json::json!({
        "card_id": card_id,
        "card_number": card_number,
        "cvv": cvv,
        "expiration_month": exp_month,
        "expiration_year": exp_year,
        "is_active": true,
        "created_at": created_at
    }))
}

pub async fn deactivate_debit_card(pool: &AnyPool, user_id: Uuid, card_id: Uuid) -> Result<()> {
    sqlx::query("UPDATE debit_cards SET is_active = FALSE WHERE id = $1 AND user_id = $2")
        .bind(card_id.to_string())
        .bind(user_id.to_string())
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to deactivate card: {}", e)))?;

    Ok(())
}

pub async fn verify_card_details(pool: &AnyPool, card_number: &str, cvv: &str, exp_month: i32, exp_year: i32, username: &str) -> Result<Option<(Uuid, Uuid)>> {
    let row = sqlx::query("
        SELECT dc.id, dc.user_id 
        FROM debit_cards dc 
        JOIN users u ON dc.user_id = u.id 
        WHERE dc.card_number = $1 
        AND dc.cvv = $2 
        AND dc.expiration_month = $3 
        AND dc.expiration_year = $4 
        AND u.username = $5 
        AND dc.is_active = TRUE
    ")
        .bind(card_number)
        .bind(cvv)
        .bind(exp_month)
        .bind(exp_year)
        .bind(username)
        .fetch_optional(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to verify card: {}", e)))?;

    if let Some(row) = row {
        let card_id = Uuid::parse_str(&row.get::<String, _>("id"))
            .map_err(|e| gurtlib::GurtError::invalid_message(format!("Invalid card ID: {}", e)))?;
        let user_id = Uuid::parse_str(&row.get::<String, _>("user_id"))
            .map_err(|e| gurtlib::GurtError::invalid_message(format!("Invalid user ID: {}", e)))?;
        Ok(Some((card_id, user_id)))
    } else {
        Ok(None)
    }
}
