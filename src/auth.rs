use crate::models::*;
use gurtlib::Result;
use serde::{Deserialize, Serialize};
use jsonwebtoken::{encode, decode, Header, Validation, EncodingKey, DecodingKey, Algorithm};
use chrono::{Duration, Utc};
use once_cell::sync::Lazy;
use sqlx::{SqlitePool, Row};
use uuid::Uuid;

static JWT_SECRET: Lazy<String> = Lazy::new(|| {
    std::env::var("JWT_SECRET").unwrap_or_else(|_| {
        "gurtpay_super_secret_key_change_in_production_2024".to_string()
    })
});

#[derive(Debug, Serialize, Deserialize)]
struct SessionClaims {
    sub: String,          // User ID
    username: String,     // Username for convenience
    exp: i64,            // Expiration timestamp
    iat: i64,            // Issued at timestamp
    session_id: String,  // Unique session identifier
}

#[derive(Debug)]
pub struct SessionToken {
    pub jwt: String,
    pub session_id: String,
    pub expires_at: chrono::DateTime<Utc>,
}

pub async fn verify_arsonflare_token(token: &str) -> Result<ArsonFlareUser> {
    println!("🔍 Verifying ArsonFlare token: {}", token);
    
    let client = reqwest::Client::new();
    let response = client
        .get("https://arsonbase.smart.is-a.dev/api/user/verify")
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("ArsonFlare request failed: {}", e)))?;
    
    println!("📡 ArsonFlare API response status: {}", response.status());
    
    if !response.status().is_success() {
        return Err(gurtlib::GurtError::invalid_message("Invalid ArsonFlare token".to_string()));
    }
    
    let user_data: serde_json::Value = response.json().await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to parse ArsonFlare response: {}", e)))?;
    
    println!("📋 ArsonFlare response JSON: {}", serde_json::to_string_pretty(&user_data).unwrap_or_else(|_| "Failed to serialize".to_string()));
    
    let user_id = user_data["sub"].as_str()
        .ok_or_else(|| gurtlib::GurtError::invalid_message("Missing 'sub' field in ArsonFlare response".to_string()))?
        .to_string();
    
    let username = user_id.clone();
    
    Ok(ArsonFlareUser {
        user_id,
        username,
    })
}

pub async fn generate_session_token(pool: &SqlitePool, user: &User) -> Result<SessionToken> {
    let session_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    let expires_at = now + Duration::hours(24);
    
    let claims = SessionClaims {
        sub: user.id.to_string(),
        username: user.username.clone(),
        exp: expires_at.timestamp(),
        iat: now.timestamp(),
        session_id: session_id.clone(),
    };
    
    let jwt = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    ).map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to generate JWT: {}", e)))?;
    
    sqlx::query(
        "INSERT INTO user_sessions (id, user_id, jwt_token, created_at, expires_at, active) 
         VALUES (?, ?, ?, ?, ?, TRUE)"
    )
    .bind(&session_id)
    .bind(user.id.to_string())
    .bind(&jwt)
    .bind(now.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to store session: {}", e)))?;
    
    Ok(SessionToken {
        jwt,
        session_id,
        expires_at,
    })
}

pub async fn validate_session_token(pool: &SqlitePool, token: &str) -> Result<User> {
    let token_data = decode::<SessionClaims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET.as_bytes()),
        &Validation::new(Algorithm::HS256),
    ).map_err(|e| gurtlib::GurtError::invalid_message(format!("Invalid JWT token: {}", e)))?;
    
    let claims = token_data.claims;
    
    let session_row = sqlx::query(
        "SELECT user_id, active, expires_at FROM user_sessions 
         WHERE id = ? AND jwt_token = ? AND active = TRUE"
    )
    .bind(&claims.session_id)
    .bind(token)
    .fetch_optional(pool)
    .await
    .map_err(|e| gurtlib::GurtError::invalid_message(format!("Database query failed: {}", e)))?;
    
    match session_row {
        Some(row) => {
            let expires_at_str: String = row.get("expires_at");
            let expires_at = chrono::DateTime::parse_from_rfc3339(&expires_at_str)
                .map_err(|_| gurtlib::GurtError::invalid_message("Invalid expiration date".to_string()))?
                .with_timezone(&Utc);
            
            if expires_at < Utc::now() {
                sqlx::query("UPDATE user_sessions SET active = FALSE WHERE id = ?")
                    .bind(&claims.session_id)
                    .execute(pool)
                    .await
                    .ok();
                
                return Err(gurtlib::GurtError::invalid_message("Session expired".to_string()));
            }
            
            let user_id = Uuid::parse_str(&claims.sub)
                .map_err(|_| gurtlib::GurtError::invalid_message("Invalid user ID in token".to_string()))?;
            
            let user_row = sqlx::query(
                "SELECT id, arsonflare_id, username, wallet_balance, wallet_address, created_at, is_admin 
                 FROM users WHERE id = ?"
            )
            .bind(user_id.to_string())
            .fetch_optional(pool)
            .await
            .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to fetch user: {}", e)))?;
            
            match user_row {
                Some(row) => {
                    Ok(User {
                        id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                        arsonflare_id: row.get("arsonflare_id"),
                        username: row.get("username"),
                        wallet_balance: row.get("wallet_balance"),
                        wallet_address: row.get("wallet_address"),
                        created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
                        is_admin: row.get("is_admin"),
                    })
                }
                None => Err(gurtlib::GurtError::invalid_message("User not found".to_string()))
            }
        }
        None => Err(gurtlib::GurtError::invalid_message("Invalid or expired session".to_string()))
    }
}

pub async fn invalidate_session(pool: &SqlitePool, token: &str) -> Result<()> {
    let token_data = decode::<SessionClaims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET.as_bytes()),
        &Validation::new(Algorithm::HS256),
    ).map_err(|e| gurtlib::GurtError::invalid_message(format!("Invalid JWT token: {}", e)))?;
    
    sqlx::query("UPDATE user_sessions SET active = FALSE WHERE id = ?")
        .bind(&token_data.claims.session_id)
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to invalidate session: {}", e)))?;
    
    Ok(())
}

pub async fn cleanup_expired_sessions(pool: &SqlitePool) -> Result<()> {
    let now = Utc::now();
    sqlx::query("UPDATE user_sessions SET active = FALSE WHERE expires_at < ? AND active = TRUE")
        .bind(now.to_rfc3339())
        .execute(pool)
        .await
        .map_err(|e| gurtlib::GurtError::invalid_message(format!("Failed to cleanup sessions: {}", e)))?;
    
    Ok(())
}

pub fn generate_code() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    
    let letters: String = (0..4)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .filter(|c| c.is_alphabetic())
        .take(4)
        .collect();
    let numbers: String = (0..4)
        .map(|_| rng.gen_range(0..10).to_string())
        .collect();
    
    format!("GC-{}-{}", letters.to_uppercase(), numbers)
}
