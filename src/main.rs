use gurtlib::prelude::*;
use gurtlib::GurtStatusCode;
use sqlx::SqlitePool;
use rustls;

mod models;
mod auth;
mod handlers;
mod database;

use handlers::*;
use database::*;

#[derive(Clone)]
pub struct AppState {
    db: SqlitePool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    
    tracing_subscriber::fmt::init();
    
    let db = init_database().await?;
    let _state = AppState { db };
    
    // Get certificate paths from environment or use defaults
    let cert_path = std::env::var("CERT_PATH").unwrap_or_else(|_| ".".to_string());
    let domain = std::env::var("GURT_DOMAIN").unwrap_or_else(|_| "localhost".to_string());
    let cert_file = format!("{}/{}.crt", cert_path, domain);
    let key_file = format!("{}/{}.key", cert_path, domain);
    
    let server = GurtServer::with_tls_certificates(&cert_file, &key_file)?
        .get("/", serve_dashboard)
        .get("/login", serve_login_page)
        .get("/register-business", serve_business_registration)
        .get("/business-manage", serve_business_manage)
        .get("/send", serve_send_page)
        .get("/wallet", serve_wallet_page)
        .get("/pay/*", serve_pay_invoice_page)
        .get("/docs", serve_api_docs)
        .get("/api-docs", serve_api_docs)
        
        .post("/api/auth/verify", handle_auth_verify)
        .post("/api/user/register", handle_user_register)
        .get("/api/user/profile", handle_get_profile)
        .get("/api/wallet/balance", handle_get_balance)
        .get("/api/wallet/transactions", handle_get_transactions)
        .post("/api/wallet/send", handle_send_money)
        .post("/api/wallet/request", handle_request_money)
        .post("/api/business/register", handle_register_business)
        .get("/api/business/list", handle_get_businesses)
        .post("/api/business/transfer", handle_business_transfer)
        .post("/api/codes/redeem", handle_redeem_code)
        .post("/api/admin/codes/create", handle_create_code)
        
        .post("/api/invoice/create", handle_create_invoice)
        .get("/api/invoice/verify/*", handle_verify_invoice)
        .get("/api/invoice/status/*", handle_get_invoice_status)
        .post("/api/invoice/pay/*", handle_pay_invoice)
        
        .get("/static/*", serve_static_files);
    
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:4878".to_string());
    let domain = std::env::var("GURT_DOMAIN").unwrap_or_else(|_| "localhost".to_string());
    
    println!("🚀 GurtPay server starting on gurt://{}:4878", domain);
    println!("💰 Virtual payment system ready!");
    println!("🔗 Binding to: {}", bind_addr);
    
    server.listen(&bind_addr).await
}

fn serve_dashboard(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = include_str!("../frontend/dashboard.html");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_login_page(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = include_str!("../frontend/login.html");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_business_registration(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = include_str!("../frontend/register-business.html");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_business_manage(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = include_str!("../frontend/business-manage.html");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_pay_invoice_page(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = include_str!("../frontend/pay-invoice.html");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_api_docs(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = include_str!("../frontend/api-docs.html");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_send_page(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = include_str!("../frontend/send.html");
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_wallet_page(_ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    Box::pin(async move {
        let html = r#"
        <head>
            <meta http-equiv="refresh" content="0; url=/" />
            <title>Redirecting...</title>
        </head>
        <body>
            <p>Redirecting to dashboard...</p>
            <script>window.location.href = "/";</script>
        </body>
        "#;
        Ok(GurtResponse::ok()
            .with_header("content-type", "text/html")
            .with_string_body(html))
    })
}

fn serve_static_files(ctx: &ServerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GurtResponse>> + Send + 'static>> {
    let path = ctx.path().to_string();
    
    Box::pin(async move {
        let path = path.strip_prefix("/static/").unwrap_or("");
        
        if path.contains("..") {
            return Ok(GurtResponse::new(GurtStatusCode::Forbidden));
        }
        
        let content = match path {
            "login.lua" => include_str!("../frontend/static/login.lua"),
            "dashboard.lua" => include_str!("../frontend/static/dashboard.lua"),
            "business.lua" => include_str!("../frontend/static/business.lua"),
            "business-manage.lua" => include_str!("../frontend/static/business-manage.lua"),
            "send.lua" => include_str!("../frontend/static/send.lua"),
            "pay-invoice.lua" => include_str!("../frontend/static/pay-invoice.lua"),
            "api-docs.lua" => include_str!("../frontend/static/api-docs.lua"),
            _ => return Ok(GurtResponse::not_found()),
        };
        
        let content_type = if path.ends_with(".css") {
            "text/css"
        } else if path.ends_with(".lua") {
            "text/plain"
        } else {
            "text/plain"
        };
        
        Ok(GurtResponse::ok()
            .with_header("content-type", content_type)
            .with_string_body(content))
    })
}
