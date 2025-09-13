local stored_session = gurt.crumbs.get("gurtpay_session")
if not stored_session then
    gurt.location.goto("/login")
end

local session_data = JSON.parse(stored_session)
local current_user = session_data.user
local session_token = session_data.session_token

gurt.select("#username").text = current_user.username

-- Get business ID from URL parameters
local url = gurt.location.href
local business_id = url:match("business_id=([^&]+)")

if not business_id then
    gurt.location.goto("/")
end

local current_business = nil

local function show_status(message, success)
    local status_el = gurt.select("#transfer-status")
    status_el.text = message
    status_el:setAttribute('style', 'text-center text-sm mb-4')
end

local function load_business_info()
    local response = fetch("/api/business/list", {
        headers = {
            ["Authorization"] = "Bearer " .. session_token
        }
    })

    if response:ok() then
        local businesses = response:json()
        for i = 1, #businesses do
            if businesses[i].id == business_id then
                current_business = businesses[i]
                break
            end
        end

        if current_business then
            local info_text = ""
            info_text = info_text .. "Business Name: " .. current_business.business_name .. "\n"
            info_text = info_text .. "Balance: " .. string.format("%.2f", current_business.balance) .. " GC\n"
            info_text = info_text .. "Merchant ID: " .. (current_business.id or business_id) .. "\n"
            info_text = info_text .. "API Key: " .. current_business.api_key .. "\n"
            if current_business.website_url and current_business.website_url ~= "" then
                info_text = info_text .. "Website: " .. current_business.website_url .. "\n"
            end
            info_text = info_text .. "Status: Verified"
            
            gurt.select("#business-info").text = info_text
        else
            gurt.location.goto("/")
        end
    else
        show_status("Failed to load business information", false)
    end
end

local function perform_transfer(direction)
    local amount_str = gurt.select("#transfer-amount").value:trim()
    local description = gurt.select("#transfer-description").value:trim()

    local amount = tonumber(amount_str)
    if not amount or amount <= 0 then
        show_status("Please enter a valid amount", false)
        return
    end

    if description == "" then
        description = direction == "deposit" and "Deposit to business" or "Withdraw from business"
    end

    local btn_id = direction == "deposit" and "#deposit-btn" or "#withdraw-btn"
    local btn = gurt.select(btn_id)
    local original_text = btn.text
    btn.text = direction == "deposit" and "Depositing..." or "Withdrawing..."
    btn.disabled = true

    local response = fetch("/api/business/transfer", {
        method = "POST",
        headers = {
            ["Authorization"] = "Bearer " .. session_token,
            ["Content-Type"] = "application/json"
        },
        body = JSON.stringify({
            business_id = business_id,
            amount = amount,
            direction = direction,
            description = description
        })
    })

    if response:ok() then
        local result = response:json()
        show_status(result.message or "Transfer completed successfully", true)
        
        -- Clear form
        gurt.select("#transfer-amount").value = ""
        gurt.select("#transfer-description").value = ""
        
        -- Reload business info to update balance
        setTimeout(function()
            load_business_info()
        end, 1000)
    else
        local msg = "Transfer failed"
        local ok_parse, parsed = pcall(function() return response:json() end)
        if ok_parse and parsed and parsed.error then
            msg = parsed.error
        end
        show_status(msg, false)
    end

    btn.text = original_text
    btn.disabled = false
end

gurt.select("#deposit-btn"):on("click", function()
    perform_transfer("deposit")
end)

gurt.select("#withdraw-btn"):on("click", function()
    perform_transfer("withdraw")
end)

load_business_info()
