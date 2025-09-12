use crate::{models::*, auth::*, database::*};
use gurtlib::prelude::*;
use gurtlib::GurtStatusCode;
use serde_json::json;
use uuid::Uuid;
use chrono::Utc;
use sqlx::Row;

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
        
        // Get user from session
        let auth_header = auth_header
            .ok_or_else(|| GurtError::invalid_message("User not authenticated".to_string()))?;
        let session_token = auth_header.strip_prefix("Bearer ")
            .ok_or_else(|| GurtError::invalid_message("Invalid authorization header format".to_string()))?;
        
        let user_session = get_user_session(&pool, session_token).await?
            .ok_or_else(|| GurtError::invalid_message("Invalid session"))?;
        
        let user_id = user_session.user_id;
        
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