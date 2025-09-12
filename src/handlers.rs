use crate::{models::*, auth::*, database::*};
use gurtlib::prelude::*;
use gurtlib::GurtStatusCode;
use serde_json::json;
use uuid::Uuid;
use chrono::Utc;
use sqlx::Row;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::{Engine, engine::general_purpose};

pub fn handle_auth_verify(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let request: AuthVerifyRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let arsonflare_user = verify_arsonflare_token(&request.token).await?;
        
        let pool = get_database_pool().await?;
        
        let user = match get_user_by_arsonflare_id(&pool, &arsonflare_user.user_id).await? {
            Some(user) => user,
            None => {
                create_user(&pool, &arsonflare_user.user_id, &arsonflare_user.username).await?
            }
        };
        
        let session_token = generate_session_token(&pool, &user).await?;
        
        let response = AuthVerifyResponse {
            user,
            session_token: session_token.jwt,
        };
        
        GurtResponse::ok().with_json_body(&response)
    })
}

pub fn handle_user_register(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let request: UserRegisterRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let arsonflare_user = verify_arsonflare_token(&request.arsonflare_token).await?;
        
        let pool = get_database_pool().await?;
        
        let user = create_user(&pool, &arsonflare_user.user_id, &arsonflare_user.username).await?;
        
        GurtResponse::ok().with_json_body(&user)
    })
}

pub fn handle_get_profile(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        GurtResponse::ok().with_json_body(&user)
    })
}

pub fn handle_get_balance(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let balance_row = sqlx::query("SELECT SUM(CASE 
                WHEN from_user_id = ? THEN -amount 
                WHEN to_user_id = ? THEN amount 
                ELSE 0 
            END) as balance FROM transactions WHERE from_user_id = ? OR to_user_id = ?")
            .bind(&user.id.to_string())
            .bind(&user.id.to_string())
            .bind(&user.id.to_string())
            .bind(&user.id.to_string())
            .fetch_one(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to get balance: {}", e)))?;
        
        let sent_row = sqlx::query("SELECT CAST(COALESCE(SUM(amount), 0) AS REAL) as total_sent FROM transactions WHERE from_user_id = ?")
            .bind(&user.id.to_string())
            .fetch_one(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to get sent total: {}", e)))?;
            
        let received_row = sqlx::query("SELECT CAST(COALESCE(SUM(amount), 0) AS REAL) as total_received FROM transactions WHERE to_user_id = ?")
            .bind(&user.id.to_string())
            .fetch_one(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to get received total: {}", e)))?;
        
        let transaction_balance: f64 = balance_row.get("balance");
        let balance = 5000.0 + transaction_balance;
        let total_sent: f64 = sent_row.get("total_sent");
        let total_received: f64 = received_row.get("total_received");
        
        let response = json!({
            "balance": balance,
            "currency": "GC",
            "address": user.wallet_address,
            "total_sent": total_sent,
            "total_received": total_received
        });
        
        GurtResponse::ok().with_json_body(&response)
    })
}

pub fn handle_get_transactions(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let rows = sqlx::query(
            "SELECT t.id, t.transaction_type, t.from_user_id, t.to_user_id, t.business_id,
                    t.amount, t.platform_fee, t.status, t.description, t.created_at, t.completed_at,
                    fu.username AS from_username, tu.username AS to_username,
                    fu.wallet_address AS from_address, tu.wallet_address AS to_address,
                    b.business_name
             FROM transactions t
             LEFT JOIN users fu ON fu.id = t.from_user_id
             LEFT JOIN users tu ON tu.id = t.to_user_id
             LEFT JOIN businesses b ON b.id = t.business_id
             WHERE t.from_user_id = ? OR t.to_user_id = ?
             ORDER BY t.created_at DESC
             LIMIT 50"
        )
        .bind(&user.id.to_string())
        .bind(&user.id.to_string())
        .fetch_all(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to get transactions: {}", e)))?;

        let transactions: Vec<_> = rows
            .into_iter()
            .map(|row| {
                let from_id: Option<String> = row.try_get::<Option<String>, _>("from_user_id").ok().flatten();
                let to_id: Option<String> = row.try_get::<Option<String>, _>("to_user_id").ok().flatten();
                let from_username: Option<String> = row.try_get::<Option<String>, _>("from_username").ok().flatten();
                let to_username: Option<String> = row.try_get::<Option<String>, _>("to_username").ok().flatten();
                let from_address: Option<String> = row.try_get::<Option<String>, _>("from_address").ok().flatten();
                let to_address: Option<String> = row.try_get::<Option<String>, _>("to_address").ok().flatten();
                let business_name: Option<String> = row.try_get::<Option<String>, _>("business_name").ok().flatten();

                let pick_non_empty = |a: Option<String>, b: Option<String>| -> Option<String> {
                    match a {
                        Some(ref s) if !s.trim().is_empty() => a,
                        _ => match b {
                            Some(ref s) if !s.trim().is_empty() => b,
                            _ => None,
                        },
                    }
                };

                let other_party: Option<String> = if let Some(bname) = business_name.filter(|s| !s.trim().is_empty()) {
                    Some(bname)
                } else if Some(user.id.to_string()) == from_id {
                    pick_non_empty(to_username.clone(), to_address.clone())
                } else {
                    pick_non_empty(from_username.clone(), from_address.clone())
                };

                json!({
                    "id": row.get::<String, _>("id"),
                    "transaction_type": row.get::<String, _>("transaction_type"),
                    "amount": row.get::<f64, _>("amount"),
                    "description": row.get::<String, _>("description"),
                    "status": row.get::<String, _>("status"),
                    "created_at": row.get::<String, _>("created_at"),
                    "from_user_id": from_id,
                    "to_user_id": to_id,
                    "other_party": other_party
                })
            })
            .collect();
        
        GurtResponse::ok().with_json_body(&transactions)
    })
}

pub fn handle_send_money(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let request: SendMoneyRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        if request.amount <= 0.0 {
            return GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Amount must be positive"}));
        }
        
        if request.amount > 10000.0 {
            return GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Amount exceeds daily limit of 10,000 GC"}));
        }
        
        let recipient = get_user_by_wallet_address(&pool, &request.to_address).await?;
        
        match recipient {
            Some(recipient) => {
                if recipient.id == user.id {
                    return GurtResponse::bad_request()
                        .with_json_body(&json!({"error": "Cannot send money to yourself"}));
                }
                
                let transaction = transfer_funds(&pool, &user.id, &recipient.id, request.amount, &request.description).await?;
                GurtResponse::ok().with_json_body(&transaction)
            }
            None => GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Recipient wallet address not found"}))
        }
    })
}

pub fn handle_request_money(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let request: RequestMoneyRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        if request.amount <= 0.0 {
            return GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Amount must be positive"}));
        }
        
        if request.amount > 10000.0 {
            return GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Amount exceeds daily limit of 10,000 GC"}));
        }
        
        let from_user = get_user_by_wallet_address(&pool, &request.from_address).await?;
        
        match from_user {
            Some(from_user) => {
                if from_user.id == user.id {
                    return GurtResponse::bad_request()
                        .with_json_body(&json!({"error": "Cannot request money from yourself"}));
                }
                
                let request_id = Uuid::new_v4();
                let created_at = Utc::now();
                
                sqlx::query(
                    "INSERT INTO money_requests (id, from_user_id, to_user_id, amount, description, status, created_at) 
                     VALUES (?, ?, ?, ?, ?, 'pending', ?)"
                )
                .bind(request_id.to_string())
                .bind(from_user.id.to_string())
                .bind(user.id.to_string())
                .bind(request.amount)
                .bind(&request.description)
                .bind(created_at.to_rfc3339())
                .execute(&pool)
                .await
                .map_err(|e| GurtError::invalid_message(format!("Failed to create money request: {}", e)))?;
                
                let response = json!({
                    "id": request_id,
                    "from_user_id": from_user.id,
                    "to_user_id": user.id,
                    "amount": request.amount,
                    "description": request.description,
                    "status": "pending",
                    "created_at": created_at.to_rfc3339()
                });
                
                GurtResponse::ok().with_json_body(&response)
            }
            None => GurtResponse::bad_request()
                .with_json_body(&json!({"error": "User wallet address not found"}))
        }
    })
}

pub fn handle_register_business(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let request: BusinessRegisterRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let business_id = Uuid::new_v4();
        let api_key = format!("gp_{}", generate_code().replace("-", "").to_lowercase());
        let created_at = Utc::now();
        
        sqlx::query(
            "INSERT INTO businesses (id, user_id, business_name, website_url, api_key, created_at) 
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(business_id.to_string())
        .bind(user.id.to_string())
        .bind(&request.business_name)
        .bind(&request.website_url)
        .bind(&api_key)
        .bind(created_at.to_rfc3339())
        .execute(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to create business: {}", e)))?;
        
        let business = Business {
            id: business_id,
            user_id: user.id,
            business_name: request.business_name,
            website_url: request.website_url,
            api_key,
            verified: true,
            balance: 0.0,
            created_at,
        };
        
        GurtResponse::ok().with_json_body(&business)
    })
}

pub fn handle_redeem_code(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let request: RedeemCodeRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let mut tx = pool.begin().await
            .map_err(|e| GurtError::invalid_message(format!("Failed to start transaction: {}", e)))?;
        
        let code_row = sqlx::query(
            "SELECT id, amount, expires_at, max_uses, current_uses, active FROM redemption_codes WHERE code = ?"
        )
        .bind(&request.code)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to check code: {}", e)))?;
        
        match code_row {
            Some(row) => {
                let code_id: String = row.get("id");
                let amount: f64 = row.get("amount");
                let expires_at: Option<String> = row.get("expires_at");
                let max_uses: Option<i32> = row.get("max_uses");
                let current_uses: i32 = row.get("current_uses");
                let active: bool = row.get("active");
                
                if !active {
                    return GurtResponse::bad_request()
                        .with_json_body(&json!({"error": "Code is not active"}));
                }
                
                if let Some(expires_str) = expires_at {
                    let expires = chrono::DateTime::parse_from_rfc3339(&expires_str)
                        .map_err(|_| GurtError::invalid_message("Invalid expiration date".to_string()))?;
                    if Utc::now() > expires {
                        return GurtResponse::bad_request()
                            .with_json_body(&json!({"error": "Code has expired"}));
                    }
                }
                
                if let Some(max) = max_uses {
                    if current_uses >= max {
                        return GurtResponse::bad_request()
                            .with_json_body(&json!({"error": "Code has reached maximum uses"}));
                    }
                }
                
                sqlx::query(
                    "UPDATE redemption_codes SET current_uses = current_uses + 1 WHERE id = ?"
                )
                .bind(&code_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| GurtError::invalid_message(format!("Failed to update code usage: {}", e)))?;
                
                let redemption_id = Uuid::new_v4();
                let redeemed_at = Utc::now();
                sqlx::query(
                    "INSERT INTO code_redemptions (id, code_id, user_id, amount_received, redeemed_at) 
                     VALUES (?, ?, ?, ?, ?)"
                )
                .bind(redemption_id.to_string())
                .bind(&code_id)
                .bind(&user.id.to_string())
                .bind(amount)
                .bind(redeemed_at.to_rfc3339())
                .execute(&mut *tx)
                .await
                .map_err(|e| GurtError::invalid_message(format!("Failed to create redemption record: {}", e)))?;
                
                let transaction_id = Uuid::new_v4();
                sqlx::query(
                    "INSERT INTO transactions (id, transaction_type, from_user_id, to_user_id, amount, platform_fee, status, description, created_at) 
                     VALUES (?, 'code_redemption', NULL, ?, ?, 0.0, 'completed', ?, ?)"
                )
                .bind(transaction_id.to_string())
                .bind(&user.id.to_string())
                .bind(amount)
                .bind(format!("Redeemed code: {}", request.code))
                .bind(redeemed_at.to_rfc3339())
                .execute(&mut *tx)
                .await
                .map_err(|e| GurtError::invalid_message(format!("Failed to create transaction: {}", e)))?;
                
                tx.commit().await
                    .map_err(|e| GurtError::invalid_message(format!("Failed to commit transaction: {}", e)))?;
                
                GurtResponse::ok().with_json_body(&json!({
                    "message": "Code redeemed successfully",
                    "amount": amount,
                    "code": request.code
                }))
            }
            None => GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Invalid or expired code"}))
        }
    })
}

pub fn handle_create_code(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        if !user.is_admin {
            return GurtResponse::new(GurtStatusCode::Forbidden)
                .with_json_body(&json!({"error": "Admin access required"}));
        }
        
        let request: CreateCodeRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let code_id = Uuid::new_v4();
        let code = generate_code();
        let created_at = Utc::now();
        let expires_at = request.expires_in_hours.map(|hours| created_at + chrono::Duration::hours(hours as i64));
        
        sqlx::query(
            "INSERT INTO redemption_codes (id, code, amount, max_uses, current_uses, created_by, created_at, expires_at, active) 
             VALUES (?, ?, ?, ?, 0, ?, ?, ?, true)"
        )
        .bind(code_id.to_string())
        .bind(&code)
        .bind(request.amount)
        .bind(request.max_uses)
        .bind(&user.id.to_string())
        .bind(created_at.to_rfc3339())
        .bind(expires_at.map(|dt| dt.to_rfc3339()))
        .execute(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to create code: {}", e)))?;
        
        let redemption_code = RedemptionCode {
            id: code_id,
            code: code.clone(),
            amount: request.amount,
            max_uses: request.max_uses,
            current_uses: 0,
            created_by: user.id,
            created_at,
            expires_at,
            active: true,
        };
        
        GurtResponse::ok().with_json_body(&redemption_code)
    })
}

pub fn handle_get_businesses(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let rows = sqlx::query(
            "SELECT id, user_id, business_name, website_url, api_key, verified, balance, created_at 
             FROM businesses WHERE user_id = ? ORDER BY created_at DESC"
        )
        .bind(&user.id.to_string())
        .fetch_all(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to get businesses: {}", e)))?;

        let businesses: Vec<Business> = rows
            .into_iter()
            .map(|row| Business {
                id: Uuid::parse_str(&row.get::<String, _>("id")).unwrap(),
                user_id: Uuid::parse_str(&row.get::<String, _>("user_id")).unwrap(),
                business_name: row.get("business_name"),
                website_url: row.get("website_url"),
                api_key: row.get("api_key"),
                verified: row.get("verified"),
                balance: row.get("balance"),
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("created_at")).unwrap().with_timezone(&Utc),
            })
            .collect();
        
        for business in &businesses {
            if !business.verified {
                let _ = sqlx::query("UPDATE businesses SET verified = TRUE WHERE id = ?")
                    .bind(&business.id.to_string())
                    .execute(&pool)
                    .await;
            }
        }

        let updated_businesses: Vec<Business> = businesses
            .into_iter()
            .map(|mut biz| {
                biz.verified = true;
                biz
            })
            .collect();

        GurtResponse::ok().with_json_body(&updated_businesses)
    })
}

pub fn handle_business_transfer(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        
        let token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let pool = get_database_pool().await?;
        
        let user = validate_session_token(&pool, token).await?;
        
        let request: BusinessTransferRequest = serde_json::from_str(&body)
            .map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        if request.amount <= 0.0 {
            return GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Amount must be positive"}));
        }
        
        let business_row = sqlx::query(
            "SELECT id, user_id, business_name, balance FROM businesses WHERE id = ? AND user_id = ?"
        )
        .bind(&request.business_id)
        .bind(&user.id.to_string())
        .fetch_optional(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to get business: {}", e)))?;
        
        let business = match business_row {
            Some(row) => row,
            None => return GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Business not found or access denied"}))
        };
        
        let business_name: String = business.get("business_name");
        let current_business_balance: f64 = business.get("balance");
        
        let mut tx = pool.begin().await
            .map_err(|e| GurtError::invalid_message(format!("Failed to start transaction: {}", e)))?;
        
        let (transaction_type, from_user_id, to_user_id, business_id_for_tx) = match request.direction.as_str() {
            "deposit" => {
                let user_balance: f64 = sqlx::query_scalar("SELECT wallet_balance FROM users WHERE id = ?")
                    .bind(&user.id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| GurtError::invalid_message(format!("Failed to get user balance: {}", e)))?;
                
                if user_balance < request.amount {
                    return GurtResponse::bad_request()
                        .with_json_body(&json!({"error": "Insufficient personal funds"}));
                }
                
                sqlx::query("UPDATE users SET wallet_balance = wallet_balance - ? WHERE id = ?")
                    .bind(request.amount)
                    .bind(&user.id.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| GurtError::invalid_message(format!("Failed to debit user: {}", e)))?;
                
                sqlx::query("UPDATE businesses SET balance = balance + ? WHERE id = ?")
                    .bind(request.amount)
                    .bind(&request.business_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| GurtError::invalid_message(format!("Failed to credit business: {}", e)))?;
                
                ("business_deposit", Some(user.id), None, Some(Uuid::parse_str(&request.business_id).unwrap()))
            },
            "withdraw" => {
                if current_business_balance < request.amount {
                    return GurtResponse::bad_request()
                        .with_json_body(&json!({"error": "Insufficient business funds"}));
                }
                
                sqlx::query("UPDATE businesses SET balance = balance - ? WHERE id = ?")
                    .bind(request.amount)
                    .bind(&request.business_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| GurtError::invalid_message(format!("Failed to debit business: {}", e)))?;
                
                sqlx::query("UPDATE users SET wallet_balance = wallet_balance + ? WHERE id = ?")
                    .bind(request.amount)
                    .bind(&user.id.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| GurtError::invalid_message(format!("Failed to credit user: {}", e)))?;
                
                ("business_withdraw", None, Some(user.id), Some(Uuid::parse_str(&request.business_id).unwrap()))
            },
            _ => return GurtResponse::bad_request()
                .with_json_body(&json!({"error": "Invalid direction. Must be 'deposit' or 'withdraw'"}))
        };
        
        let transaction_id = Uuid::new_v4();
        let created_at = Utc::now();
        
        sqlx::query(
            "INSERT INTO transactions (id, transaction_type, from_user_id, to_user_id, business_id, amount, platform_fee, status, description, created_at, completed_at) 
             VALUES (?, ?, ?, ?, ?, ?, 0.0, 'completed', ?, ?, ?)"
        )
        .bind(transaction_id.to_string())
        .bind(transaction_type)
        .bind(from_user_id.map(|id| id.to_string()))
        .bind(to_user_id.map(|id| id.to_string()))
        .bind(business_id_for_tx.map(|id| id.to_string()))
        .bind(request.amount)
        .bind(&format!("{} - {}", request.description, business_name))
        .bind(created_at.to_rfc3339())
        .bind(created_at.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to create transaction: {}", e)))?;
        
        tx.commit().await
            .map_err(|e| GurtError::invalid_message(format!("Failed to commit transaction: {}", e)))?;
        
        GurtResponse::ok().with_json_body(&json!({
            "message": format!("Successfully {} {} GC {}", 
                              if request.direction == "deposit" { "deposited" } else { "withdrew" },
                              request.amount,
                              if request.direction == "deposit" { "to" } else { "from" }),
            "transaction_id": transaction_id,
            "amount": request.amount,
            "direction": request.direction
        }))
    })
}

pub fn handle_create_invoice(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let body = ctx.text().unwrap_or_default();
    let headers = ctx.headers().clone();
    
    Box::pin(async move {
        let auth_header = headers.get("authorization")
            .ok_or_else(|| GurtError::invalid_message("Missing Authorization header"))?;
        
        let api_key = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid Authorization header format"))?;
        
        let pool = get_database_pool().await?;
        
        let business = get_business_by_api_key(&pool, api_key).await?
            .ok_or_else(|| GurtError::invalid_message("Invalid API key"))?;
        
        let req: CreateInvoiceRequest = serde_json::from_str(&body)
            .map_err(|e| GurtError::invalid_message(format!("Invalid JSON: {}", e)))?;
        
        if req.amount <= 0.0 {
            return Err(GurtError::invalid_message("Amount must be greater than 0"));
        }
        
        let expires_at = Some(Utc::now() + chrono::Duration::hours(req.expires_in_hours.unwrap_or(24) as i64));
        
        let invoice = create_invoice(
            &pool,
            business.id,
            req.amount,
            &req.description,
            req.customer_name.as_deref(),
            expires_at,
        ).await?;
        
        let payment_url = format!("gurt://gurtpay.dev/pay/{}", invoice.id);
        
        let response = CreateInvoiceResponse {
            invoice_id: invoice.id,
            payment_url,
            amount: invoice.amount,
            description: invoice.description,
            status: invoice.status,
            expires_at: invoice.expires_at,
        };
        
        GurtResponse::ok().with_json_body(&json!(response))
    })
}

pub fn handle_verify_invoice(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let path = ctx.path().to_string();
    let headers = ctx.headers().clone();
    
    Box::pin(async move {
        let invoice_id_str = path.strip_prefix("/api/invoice/verify/")
            .ok_or_else(|| GurtError::invalid_message("Missing invoice ID in path"))?;
        
        let invoice_id = Uuid::parse_str(invoice_id_str)
            .map_err(|_| GurtError::invalid_message("Invalid invoice ID format"))?;
        
        let auth_header = headers.get("authorization")
            .ok_or_else(|| GurtError::invalid_message("Missing Authorization header"))?;
        
        let api_key = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid Authorization header format"))?;
        
        let pool = get_database_pool().await?;
        
        let business = get_business_by_api_key(&pool, api_key).await?
            .ok_or_else(|| GurtError::invalid_message("Invalid API key"))?;
        
        let invoice = get_invoice(&pool, invoice_id).await?
            .ok_or_else(|| GurtError::invalid_message("Invoice not found"))?;
        
        if invoice.business_id != business.id {
            return Err(GurtError::invalid_message("Invoice does not belong to this business"));
        }
        
        GurtResponse::ok().with_json_body(&json!(invoice))
    })
}

pub fn handle_pay_invoice(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let path = ctx.path().to_string();
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    
    Box::pin(async move {
        let invoice_id_str = path.strip_prefix("/api/invoice/pay/")
            .ok_or_else(|| GurtError::invalid_message("Missing invoice ID in path"))?;
        
        let invoice_id = Uuid::parse_str(invoice_id_str)
            .map_err(|_| GurtError::invalid_message("Invalid invoice ID format"))?;
        
        let pool = get_database_pool().await?;
        
        // Validate user via JWT (stored as jwt_token in user_sessions)
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("User not authenticated".to_string()))?;
        let session_token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;

        let user = validate_session_token(&pool, session_token).await?;
        let user_id = user.id;
        
        let invoice = get_invoice(&pool, invoice_id).await?
            .ok_or_else(|| GurtError::invalid_message("Invoice not found"))?;
        
        if matches!(invoice.status, InvoiceStatus::Paid) {
            return Err(GurtError::invalid_message("Invoice is already paid"));
        }
        
        if let Some(expires_at) = invoice.expires_at {
            if Utc::now() > expires_at {
                return Err(GurtError::invalid_message("Invoice has expired"));
            }
        }
        
        let user = get_user_by_id(&pool, user_id).await?
            .ok_or_else(|| GurtError::invalid_message("User not found"))?;
        
        if user.wallet_balance < invoice.amount {
            return Err(GurtError::invalid_message("Insufficient balance"));
        }
        
        transfer_to_business(&pool, &user.id, &invoice.business_id, invoice.amount,
                             &format!("Payment for invoice: {}", invoice.description)).await?;
        
        mark_invoice_paid(&pool, invoice.id).await?;
        
        GurtResponse::ok().with_json_body(&json!({"status": "paid", "message": "Payment successful"}))
    })
}

pub fn handle_get_invoice_status(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let path = ctx.path().to_string();
    
    Box::pin(async move {
        let invoice_id_str = path.strip_prefix("/api/invoice/status/")
            .ok_or_else(|| GurtError::invalid_message("Missing invoice ID in path"))?;
        
        let invoice_id = Uuid::parse_str(invoice_id_str)
            .map_err(|_| GurtError::invalid_message("Invalid invoice ID format"))?;
        
        let pool = get_database_pool().await?;
        
        let invoice = get_invoice(&pool, invoice_id).await?
            .ok_or_else(|| GurtError::invalid_message("Invoice not found"))?;
        
        let business = get_business_by_id(&pool, invoice.business_id).await?
            .ok_or_else(|| GurtError::invalid_message("Business not found"))?;
        
        GurtResponse::ok().with_json_body(&json!({
            "invoice": invoice,
            "business": {
                "business_name": business.business_name,
                "website_url": business.website_url
            }
        }))
    })
}

// ======== ADS: Bootstrap and Serving ========

fn ads_secret() -> String {
    std::env::var("ADS_SECRET").unwrap_or_else(|_| "gurtpay_ads_secret_change_me".to_string())
}

fn sign_token(data: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(ads_secret().as_bytes()).unwrap();
    mac.update(data.as_bytes());
    let sig = mac.finalize().into_bytes();
    general_purpose::URL_SAFE_NO_PAD.encode(sig)
}

pub fn handle_ads_bootstrap(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let lua = include_str!("../frontend/static/ads-bootstrap.lua");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/plain")
            .with_string_body(lua))
    })
}

fn hash_ip(ip: &str) -> String {
    let salt = ads_secret();
    let mut mac = Hmac::<Sha256>::new_from_slice(salt.as_bytes()).unwrap();
    mac.update(ip.as_bytes());
    let sig = mac.finalize().into_bytes();
    general_purpose::URL_SAFE_NO_PAD.encode(&sig[0..8])
}

fn hash_device(device_id: &str) -> String {
    let salt = ads_secret();
    let mut mac = Hmac::<Sha256>::new_from_slice(salt.as_bytes()).unwrap();
    mac.update(device_id.as_bytes());
    let sig = mac.finalize().into_bytes();
    general_purpose::URL_SAFE_NO_PAD.encode(&sig[0..8])
}

pub fn handle_ads_serve(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let query = ctx.path().split('?').nth(1).unwrap_or("").to_string();
    let client_ip = ctx.header("x-forwarded-for").or_else(|| ctx.header("x-real-ip")).map_or("unknown", |v| v).to_string();
    Box::pin(async move {
        // Parse query params manually since UrlEncoded not available
        let mut site_id = None;
        let mut slot_key = None;
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "site_id" => site_id = Some(value.to_string()),
                    "slot_key" => slot_key = Some(value.to_string()),
                    _ => {}
                }
            }
        }
        let site_id = site_id.ok_or_else(|| GurtError::invalid_message("Missing site_id".to_string()))?;
        let slot_key = slot_key.ok_or_else(|| GurtError::invalid_message("Missing slot_key".to_string()))?;

        let pool = get_database_pool().await?;

        let slot_row = sqlx::query(
            "SELECT s.id as slot_id, s.site_id, s.format, s.width, s.height, si.verified FROM ads_slots s JOIN ads_sites si ON s.site_id = si.id WHERE s.slot_key = ?"
        )
        .bind(&slot_key)
        .fetch_optional(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to get slot: {}", e)))?;

        let slot = match slot_row { Some(r) => r, None => return Ok(GurtResponse::not_found()) };
        let verified: bool = slot.get("verified");
        if !verified { return Ok(GurtResponse::forbidden()); }

        // Select an active creative that matches slot format and has budget
        let creative_row = sqlx::query(
            "SELECT c.id, c.format, c.width, c.height, c.html, c.image_url, c.click_url, c.campaign_id
             FROM ads_creatives c JOIN ads_campaigns a ON c.campaign_id = a.id
             WHERE c.status = 'active' AND a.status = 'active' AND a.budget_remaining > 0
               AND (c.format = (SELECT format FROM ads_slots WHERE slot_key = ?) OR c.format = 'any')
             ORDER BY a.budget_remaining DESC LIMIT 1"
        )
        .bind(&slot_key)
        .fetch_optional(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to pick creative: {}", e)))?;

        let creative = match creative_row { Some(r) => r, None => return GurtResponse::ok().with_json_body(&json!({"no_fill": true})) };

        let token_id = Uuid::new_v4();
        let now = Utc::now();
        let exp = now + chrono::Duration::minutes(5);
        let token_payload = format!("{}|{}|{}|{}", slot.get::<String,_>("site_id"), slot.get::<String,_>("slot_id"), creative.get::<String,_>("campaign_id"), creative.get::<String,_>("id"));
        let token_sig = sign_token(&token_payload);
        let token = format!("{}.{}", token_payload, token_sig);

        sqlx::query(
            "INSERT INTO ads_tokens (id, token, site_id, slot_id, campaign_id, creative_id, exp, used, issued_at) VALUES (?, ?, ?, ?, ?, ?, ?, FALSE, ?)"
        )
        .bind(token_id.to_string())
        .bind(&token)
        .bind(slot.get::<String,_>("site_id"))
        .bind(slot.get::<String,_>("slot_id"))
        .bind(creative.get::<String,_>("campaign_id"))
        .bind(creative.get::<String,_>("id"))
        .bind(exp.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to store token: {}", e)))?;

        let response = json!({
            "token": token,
            "creative": {
                "format": creative.get::<String,_>("format"),
                "width": creative.get::<Option<i64>,_>("width"),
                "height": creative.get::<Option<i64>,_>("height"),
                "html": creative.get::<Option<String>,_>("html"),
                "image_url": creative.get::<Option<String>,_>("image_url"),
                "click": format!("/api/ads/click/{}", token_id),
            }
        });
        GurtResponse::ok().with_json_body(&response)
    })
}

pub fn handle_ads_beacon_start(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let body = ctx.text().unwrap_or_default();
    Box::pin(async move {
        let payload: serde_json::Value = serde_json::from_str(&body).map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        let token: String = payload["token"].as_str().ok_or_else(|| GurtError::invalid_message("Missing token".to_string()))?.to_string();
        let device_hash: Option<String> = payload["device_hash"].as_str().map(|s| s.to_string());

        let pool = get_database_pool().await?;
        let row = sqlx::query("SELECT id, exp, used FROM ads_tokens WHERE token = ?")
            .bind(&token)
            .fetch_optional(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to read token: {}", e)))?;
        let r = match row { Some(r) => r, None => return Ok(GurtResponse::bad_request().with_string_body("Invalid token")) };
        if r.get::<i64,_>("used") != 0 { return Ok(GurtResponse::bad_request().with_string_body("Used token")); }
        let exp = chrono::DateTime::parse_from_rfc3339(&r.get::<String,_>("exp")).unwrap().with_timezone(&Utc);
        if Utc::now() > exp { return Ok(GurtResponse::bad_request().with_string_body("Expired token")); }

        let impression_id = Uuid::new_v4();
        sqlx::query("UPDATE ads_tokens SET used = TRUE WHERE id = ?")
            .bind(r.get::<String,_>("id"))
            .execute(&pool).await.ok();

        sqlx::query("INSERT INTO ads_impressions (id, token_id, site_id, slot_id, campaign_id, creative_id, device_hash, ip_hash, started_at, status) \
                     SELECT ?, t.id, t.site_id, t.slot_id, t.campaign_id, t.creative_id, ?, ?, ?, 'started' FROM ads_tokens t WHERE t.id = ?")
            .bind(impression_id.to_string())
            .bind(device_hash.clone())
            .bind(hash_ip("unknown"))
            .bind(Utc::now().to_rfc3339())
            .bind(r.get::<String,_>("id"))
            .execute(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to start impression: {}", e)))?;

        GurtResponse::ok().with_json_body(&json!({"ok": true, "impression_id": impression_id}))
    })
}

pub fn handle_ads_beacon_viewable(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let body = ctx.text().unwrap_or_default();
    Box::pin(async move {
        let payload: serde_json::Value = serde_json::from_str(&body).map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        let impression_id = payload["impression_id"].as_str().ok_or_else(|| GurtError::invalid_message("Missing impression_id".to_string()))?;
        let ms_visible = payload["ms_visible"].as_i64().unwrap_or(0);
        if ms_visible < 1000 { return Ok(GurtResponse::bad_request().with_string_body("Too short")); }

        let pool = get_database_pool().await?;
        // Read impression with joins to resolve campaign and site owner
        let row = sqlx::query(
            "SELECT i.id, i.campaign_id, i.site_id, i.creative_id, a.bid_model, a.max_cpm, a.max_cpc, a.advertiser_business_id, s.owner_business_id \
             FROM ads_impressions i \
             JOIN ads_tokens t ON t.id = i.token_id \
             JOIN ads_campaigns a ON a.id = i.campaign_id \
             JOIN ads_sites s ON s.id = i.site_id \
             WHERE i.id = ?"
        )
        .bind(impression_id)
        .fetch_one(&pool)
        .await
        .map_err(|e| GurtError::invalid_message(format!("Failed to read impression: {}", e)))?;

        let bid_model: String = row.get("bid_model");
        let _advertiser: String = row.get("advertiser_business_id");
        let host: String = row.get("owner_business_id");

        let mut tx = get_database_pool().await?.begin().await
            .map_err(|e| GurtError::invalid_message(format!("Start tx failed: {}", e)))?;

        // Pricing: CPM charge at finalize
        let mut cost = 0.0f64;
        if bid_model == "cpm" {
            let max_cpm: f64 = row.get("max_cpm");
            cost = max_cpm / 1000.0;
        }

        // Budget check
        let remaining: f64 = sqlx::query_scalar("SELECT budget_remaining FROM ads_campaigns WHERE id = ?")
            .bind(row.get::<String,_>("campaign_id"))
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to get budget: {}", e)))?;
        if remaining < cost { return Ok(GurtResponse::bad_request().with_string_body("Insufficient budget")); }

        // Basic dedupe: check if same device+creative in last hour 
        let device_hash_val = payload["device_hash"].as_str().unwrap_or("");
        if !device_hash_val.is_empty() {
            let one_hour_ago = Utc::now() - chrono::Duration::hours(1);
            let existing = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM ads_impressions WHERE device_hash = ? AND creative_id = ? AND finalized_at > ?")
                .bind(device_hash_val)
                .bind(row.get::<String,_>("creative_id"))
                .bind(one_hour_ago.to_rfc3339())
                .fetch_one(&mut *tx)
                .await
                .unwrap_or(0);
            if existing > 0 { return Ok(GurtResponse::bad_request().with_string_body("Duplicate impression")); }
        }

        // Debit advertiser budget
        sqlx::query("UPDATE ads_campaigns SET budget_remaining = budget_remaining - ? WHERE id = ?")
            .bind(cost)
            .bind(row.get::<String,_>("campaign_id"))
            .execute(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Budget debit failed: {}", e)))?;

        // Credit host business (90%), platform fee (10%)
        let host_amount = cost * 0.9;
        let _platform_fee = cost - host_amount;

        sqlx::query("UPDATE businesses SET balance = balance + ? WHERE id = ?")
            .bind(host_amount)
            .bind(&host)
            .execute(&mut *tx).await.ok();

        // Record impression cost
        sqlx::query("UPDATE ads_impressions SET viewable_at = ?, finalized_at = ?, status = 'viewable', cost_micros = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(Utc::now().to_rfc3339())
            .bind((cost * 1_000_000.0) as i64)
            .bind(impression_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Finalize write failed: {}", e)))?;

        tx.commit().await.ok();
        GurtResponse::ok().with_json_body(&json!({"ok": true, "cost": cost}))
    })
}

pub fn handle_ads_click(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let path = ctx.path().to_string();
    Box::pin(async move {
        let id = path.strip_prefix("/api/ads/click/").ok_or_else(|| GurtError::invalid_message("Missing id"))?;
        let pool = get_database_pool().await?;
        // Mark click timestamp if impression exists
        let _ = sqlx::query("UPDATE ads_impressions SET click_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(id)
            .execute(&pool).await;
        Ok(GurtResponse::new(GurtStatusCode::NoContent))
    })
}

// ======== ADS: Management Endpoints ========

pub fn handle_ads_register_site(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header.ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        let pool = get_database_pool().await?;
        let user = validate_session_token(&pool, token).await?;
        
        let request: RegisterSiteRequest = serde_json::from_str(&body).map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let business_row = sqlx::query("SELECT id FROM businesses WHERE user_id = ? LIMIT 1")
            .bind(user.id.to_string())
            .fetch_optional(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to get business: {}", e)))?;
        let business_id = match business_row { Some(r) => r.get::<String,_>("id"), None => return Ok(GurtResponse::bad_request().with_string_body("No business found")) };
        
        let site_id = Uuid::new_v4();
        let verification_token = Uuid::new_v4().to_string();
        let created_at = Utc::now();
        
        sqlx::query("INSERT INTO ads_sites (id, owner_business_id, domain, verified, verification_token, created_at) VALUES (?, ?, ?, FALSE, ?, ?)")
            .bind(site_id.to_string())
            .bind(&business_id)
            .bind(&request.domain)
            .bind(&verification_token)
            .bind(created_at.to_rfc3339())
            .execute(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to create site: {}", e)))?;
        
        let site = AdsSite {
            id: site_id,
            owner_business_id: Uuid::parse_str(&business_id).unwrap(),
            domain: request.domain,
            verified: false,
            verification_token,
            created_at,
        };
        
        GurtResponse::ok().with_json_body(&site)
    })
}

pub fn handle_ads_register_slot(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header.ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        let pool = get_database_pool().await?;
        let user = validate_session_token(&pool, token).await?;
        
        let request: RegisterSlotRequest = serde_json::from_str(&body).map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let site_row = sqlx::query("SELECT s.id FROM ads_sites s JOIN businesses b ON s.owner_business_id = b.id WHERE s.id = ? AND b.user_id = ?")
            .bind(&request.site_id)
            .bind(user.id.to_string())
            .fetch_optional(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to verify site ownership: {}", e)))?;
        match site_row { Some(_) => {}, None => return Ok(GurtResponse::forbidden()) };
        
        let slot_id = Uuid::new_v4();
        sqlx::query("INSERT INTO ads_slots (id, site_id, slot_key, format, width, height, floor_price, active) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)")
            .bind(slot_id.to_string())
            .bind(&request.site_id)
            .bind(&request.slot_key)
            .bind(&request.format)
            .bind(request.width)
            .bind(request.height)
            .bind(request.floor_price.unwrap_or(0.0))
            .execute(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to create slot: {}", e)))?;
        
        let slot = AdsSlot {
            id: slot_id,
            site_id: Uuid::parse_str(&request.site_id).unwrap(),
            slot_key: request.slot_key,
            format: request.format,
            width: request.width,
            height: request.height,
            floor_price: request.floor_price.unwrap_or(0.0),
            active: true,
        };
        
        GurtResponse::ok().with_json_body(&slot)
    })
}

pub fn handle_ads_create_campaign(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header.ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        let pool = get_database_pool().await?;
        let user = validate_session_token(&pool, token).await?;
        
        let request: CreateCampaignRequest = serde_json::from_str(&body).map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        if request.budget_total <= 0.0 { return Ok(GurtResponse::bad_request().with_string_body("Budget must be positive")); }
        if request.bid_model != "cpm" && request.bid_model != "cpc" { return Ok(GurtResponse::bad_request().with_string_body("Invalid bid model")); }
        
        let business_row = sqlx::query("SELECT id FROM businesses WHERE user_id = ? LIMIT 1")
            .bind(user.id.to_string())
            .fetch_optional(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to get business: {}", e)))?;
        let business_id = match business_row { Some(r) => r.get::<String,_>("id"), None => return Ok(GurtResponse::bad_request().with_string_body("No business found")) };
        
        let campaign_id = Uuid::new_v4();
        let created_at = Utc::now();
        
        sqlx::query("INSERT INTO ads_campaigns (id, advertiser_business_id, budget_total, budget_remaining, bid_model, max_cpm, max_cpc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)")
            .bind(campaign_id.to_string())
            .bind(&business_id)
            .bind(request.budget_total)
            .bind(0.0) // budget_remaining starts at 0, must be funded separately
            .bind(&request.bid_model)
            .bind(request.max_cpm)
            .bind(request.max_cpc)
            .bind(created_at.to_rfc3339())
            .execute(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to create campaign: {}", e)))?;
        
        let campaign = AdsCampaign {
            id: campaign_id,
            advertiser_business_id: Uuid::parse_str(&business_id).unwrap(),
            budget_total: request.budget_total,
            budget_remaining: 0.0,
            bid_model: request.bid_model,
            max_cpm: request.max_cpm,
            max_cpc: request.max_cpc,
            status: "active".to_string(),
            created_at,
        };
        
        GurtResponse::ok().with_json_body(&campaign)
    })
}

pub fn handle_ads_create_creative(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header.ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        let pool = get_database_pool().await?;
        let user = validate_session_token(&pool, token).await?;
        
        let request: CreateCreativeRequest = serde_json::from_str(&body).map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        let campaign_row = sqlx::query("SELECT c.id FROM ads_campaigns c JOIN businesses b ON c.advertiser_business_id = b.id WHERE c.id = ? AND b.user_id = ?")
            .bind(&request.campaign_id)
            .bind(user.id.to_string())
            .fetch_optional(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to verify campaign ownership: {}", e)))?;
        match campaign_row { Some(_) => {}, None => return Ok(GurtResponse::forbidden()) };
        
        let creative_id = Uuid::new_v4();
        sqlx::query("INSERT INTO ads_creatives (id, campaign_id, format, width, height, html, image_url, click_url, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')")
            .bind(creative_id.to_string())
            .bind(&request.campaign_id)
            .bind(&request.format)
            .bind(request.width)
            .bind(request.height)
            .bind(&request.html)
            .bind(&request.image_url)
            .bind(&request.click_url)
            .execute(&pool)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to create creative: {}", e)))?;
        
        let creative = AdsCreative {
            id: creative_id,
            campaign_id: Uuid::parse_str(&request.campaign_id).unwrap(),
            format: request.format,
            width: request.width,
            height: request.height,
            html: request.html,
            image_url: request.image_url,
            click_url: request.click_url,
            status: "active".to_string(),
        };
        
        GurtResponse::ok().with_json_body(&creative)
    })
}

pub fn handle_ads_fund_campaign(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let auth_header = ctx.header("authorization").map(|s| s.to_string());
    let body = ctx.text().unwrap_or_default();
    
    Box::pin(async move {
        let auth_header = auth_header.ok_or_else(|| GurtError::invalid_message("Missing authorization header".to_string()))?;
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        let pool = get_database_pool().await?;
        let user = validate_session_token(&pool, token).await?;
        
        let request: FundCampaignRequest = serde_json::from_str(&body).map_err(|_| GurtError::invalid_message("Invalid JSON".to_string()))?;
        
        if request.amount <= 0.0 { return Ok(GurtResponse::bad_request().with_string_body("Amount must be positive")); }
        
        let mut tx = pool.begin().await.map_err(|e| GurtError::invalid_message(format!("Transaction start failed: {}", e)))?;
        
        let campaign_row = sqlx::query("SELECT c.id, c.advertiser_business_id FROM ads_campaigns c JOIN businesses b ON c.advertiser_business_id = b.id WHERE c.id = ? AND b.user_id = ?")
            .bind(&request.campaign_id)
            .bind(user.id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to verify campaign ownership: {}", e)))?;
        let campaign = match campaign_row { Some(r) => r, None => return Ok(GurtResponse::forbidden()) };
        let business_id: String = campaign.get("advertiser_business_id");
        
        let business_balance: f64 = sqlx::query_scalar("SELECT balance FROM businesses WHERE id = ?")
            .bind(&business_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to get business balance: {}", e)))?;
        
        if business_balance < request.amount { return Ok(GurtResponse::bad_request().with_string_body("Insufficient business balance")); }
        
        sqlx::query("UPDATE businesses SET balance = balance - ? WHERE id = ?")
            .bind(request.amount)
            .bind(&business_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to debit business: {}", e)))?;
        
        sqlx::query("UPDATE ads_campaigns SET budget_remaining = budget_remaining + ? WHERE id = ?")
            .bind(request.amount)
            .bind(&request.campaign_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to fund campaign: {}", e)))?;
        
        let transaction_id = Uuid::new_v4();
        let created_at = Utc::now();
        sqlx::query("INSERT INTO transactions (id, transaction_type, from_user_id, to_user_id, business_id, amount, platform_fee, status, description, created_at, completed_at) VALUES (?, 'ads_fund', ?, NULL, ?, ?, 0.0, 'completed', ?, ?, ?)")
            .bind(transaction_id.to_string())
            .bind(user.id.to_string())
            .bind(&business_id)
            .bind(request.amount)
            .bind(format!("Ads campaign funding: {}", request.campaign_id))
            .bind(created_at.to_rfc3339())
            .bind(created_at.to_rfc3339())
            .execute(&mut *tx)
            .await
            .map_err(|e| GurtError::invalid_message(format!("Failed to log transaction: {}", e)))?;
        
        tx.commit().await.map_err(|e| GurtError::invalid_message(format!("Commit failed: {}", e)))?;
        
        GurtResponse::ok().with_json_body(&json!({"ok": true, "funded": request.amount}))
    })
}