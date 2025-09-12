use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub arsonflare_id: String,
    pub username: String,
    pub wallet_balance: f64,
    pub wallet_address: String,
    pub created_at: DateTime<Utc>,
    pub is_admin: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Business {
    pub id: Uuid,
    pub user_id: Uuid,
    pub business_name: String,
    pub website_url: Option<String>,
    pub api_key: String,
    pub verified: bool,
    pub balance: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: Uuid,
    pub transaction_type: TransactionType,
    pub from_user_id: Option<Uuid>,
    pub to_user_id: Option<Uuid>,
    pub business_id: Option<Uuid>,
    pub amount: f64,
    pub platform_fee: f64,
    pub status: TransactionStatus,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    Transfer,
    BusinessPayment,
    CodeRedemption,
    PlatformFee,
    Welcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    Pending,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedemptionCode {
    pub id: Uuid,
    pub code: String,
    pub amount: f64,
    pub max_uses: Option<i32>,
    pub current_uses: i32,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeRedemption {
    pub id: Uuid,
    pub code_id: Uuid,
    pub user_id: Uuid,
    pub amount_received: f64,
    pub redeemed_at: DateTime<Utc>,
}

// Request/Response DTOs
#[derive(Debug, Deserialize)]
pub struct AuthVerifyRequest {
    pub token: String,
}

#[derive(Debug, Serialize)]
pub struct AuthVerifyResponse {
    pub user: User,
    pub session_token: String,
}

#[derive(Debug, Deserialize)]
pub struct UserRegisterRequest {
    pub arsonflare_token: String,
}

#[derive(Debug, Deserialize)]
pub struct SendMoneyRequest {
    pub to_address: String,
    pub amount: f64,
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct RequestMoneyRequest {
    pub from_address: String,
    pub amount: f64,
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct BusinessRegisterRequest {
    pub business_name: String,
    pub website_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RedeemCodeRequest {
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateCodeRequest {
    pub amount: f64,
    pub max_uses: Option<i32>,
    pub expires_in_hours: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct BusinessTransferRequest {
    pub business_id: String,
    pub amount: f64,
    pub direction: String, // "deposit" or "withdraw"
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoice {
    pub id: Uuid,
    pub business_id: Uuid,
    pub amount: f64,
    pub description: String,
    pub customer_name: Option<String>,
    pub status: InvoiceStatus,
    pub paid_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvoiceStatus {
    Pending,
    Paid,
    Expired,
    Cancelled,
}

#[derive(Debug, Deserialize)]
pub struct CreateInvoiceRequest {
    pub amount: f64,
    pub description: String,
    pub customer_name: Option<String>,
    pub expires_in_hours: Option<i32>, // Default 24 hours
}

#[derive(Debug, Serialize)]
pub struct CreateInvoiceResponse {
    pub invoice_id: Uuid,
    pub payment_url: String,
    pub amount: f64,
    pub description: String,
    pub status: InvoiceStatus,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct CreateCodeResponse {
    pub code: String,
    pub amount: f64,
    pub max_uses: Option<i32>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct WalletInfo {
    pub balance: f64,
    pub address: String,
    pub total_sent: f64,
    pub total_received: f64,
}

#[derive(Debug, Serialize)]
pub struct TransactionSummary {
    pub id: Uuid,
    pub transaction_type: TransactionType,
    pub amount: f64,
    pub description: String,
    pub status: TransactionStatus,
    pub created_at: DateTime<Utc>,
    pub other_party: Option<String>, // username or business name
}

// ArsonFlare OAuth response
#[derive(Debug, Deserialize)]
pub struct ArsonFlareUser {
    pub user_id: String,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub session_token: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
