local token = gurt.crumbs.get("gurtpay_session")
if not token then 
    gurt.location.goto("/login")
end

local session = JSON.parse(token)
local session_token = session.session_token

local wallet_info = {}

local function render_wallet()
    local text_content = ""
    text_content = text_content .. "Address: " .. (wallet_info.address or "N/A") .. "\n\n"
    text_content = text_content .. "Balance: " .. tostring(wallet_info.balance or 0) .. " GC" .. "\n\n"
    text_content = text_content .. "Total Sent: " .. tostring(wallet_info.total_sent or 0) .. " GC" .. "\n\n"
    text_content = text_content .. "Total Received: " .. tostring(wallet_info.total_received or 0) .. " GC"
    gurt.select('#wallet-info').text = text_content
end

local function fetch_wallet()
    local response = fetch('/api/wallet/balance', {
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        wallet_info = response:json()
        gurt.select('#username').text = "Welcome, " .. (session.user and session.user.username or "User")
        render_wallet()
    else
        gurt.select('#wallet-info').text = "Failed to load wallet info: " .. response:text()
    end
end

local transactions_list = {}

local function render_transactions()
    local text_content = ""
    if #transactions_list == 0 then
        text_content = "No transactions yet."
    else
        for i = 1, math.min(#transactions_list, 10) do
            local tx = transactions_list[i]
            local sent = session.user and tx.from_user_id == session.user.id
            local prefix = sent and "-" or "+"
            local party_label = tx.other_party and (sent and ("to " .. tx.other_party) or ("from " .. tx.other_party)) or ""
            
            text_content = text_content .. (tx.description or "No description")
            text_content = text_content .. "  " .. (tx.transaction_type or "unknown"):gsub("_", " "):upper()
            if party_label ~= "" then
                text_content = text_content .. "  (" .. party_label .. ")"
            end
            text_content = text_content .. "  " .. prefix .. string.format("%.2f", tx.amount or 0) .. " GC"
            text_content = text_content .. "  " .. (tx.created_at or "Unknown"):sub(1, 16):gsub("T", " ") .. "\n\n"
        end
    end
    gurt.select('#transactions-list').text = text_content
end

local function fetch_transactions()
    local response = fetch('/api/wallet/transactions', {
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        transactions_list = response:json()
        render_transactions()
    else
        gurt.select('#transactions-list').text = "Failed to load transactions: " .. response:text()
    end
end

local businesses_list = {}

local function render_businesses()
    local grid = gurt.select('#businesses-grid')
    if not grid then return end
    
    -- Clear previous children
    local children = grid.children
    for i = #children, 1, -1 do
        children[i]:remove()
    end
    
    if #businesses_list == 0 then
        local empty = gurt.create('p', {
            text = 'No businesses registered yet.',
            style = 'text-sm text-slate-700 bg-[#f9fafb] p-4 rounded border'
        })
        grid:append(empty)
        return
    end
    
    for _, biz in ipairs(businesses_list) do
        local card = gurt.create('div', {
            style = 'w-[300px] min-w-[300px] max-w-[300px] bg-[#f9fafb] rounded-lg border border-slate-200 p-4 shadow-sm flex flex-col gap-3'
        })
        
        local title = gurt.create('h3', {
            text = '🏢 ' .. (biz.business_name or 'Unknown Business'),
            style = 'font-bold text-lg text-slate-900'
        })
        
        local balance = gurt.create('p', {
            text = '💰 Balance: ' .. string.format("%.2f", biz.balance or 0) .. ' GC',
            style = 'text-sm text-slate-600 font-medium'
        })
        
        local api_key = gurt.create('p', {
            text = '🔑 ' .. (biz.api_key or 'N/A'),
            style = 'text-xs text-slate-500 font-mono'
        })
        
        local status = gurt.create('span', {
            text = '✅ Verified',
            style = 'inline-block bg-green-100 text-green-800 text-xs px-2 py-1 rounded'
        })
        
        local buttons_container = gurt.create('div', {
            style = 'flex gap-2 mt-2'
        })
        
        local manage_btn = gurt.create('button', {
            text = 'Manage',
            style = 'flex-1 bg-slate-600 text-white px-3 py-2 rounded text-sm hover:bg-slate-700'
        })
        manage_btn:on('click', function()
            gurt.location.goto('/business-manage?business_id=' .. biz.id)
        end)
        
        buttons_container:append(manage_btn)
        
        card:append(title)
        card:append(balance)
        card:append(api_key)
        if biz.website_url and biz.website_url ~= "" then
            local website = gurt.create('p', {
                text = '🌐 ' .. biz.website_url,
                style = 'text-xs text-slate-500'
            })
            card:append(website)
        end
        card:append(status)
        card:append(buttons_container)
        
        grid:append(card)
    end
end

local function fetch_businesses()
    local response = fetch('/api/business/list', {
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        businesses_list = response:json()
        render_businesses()
    else
        gurt.select('#businesses-list').text = "Failed to load businesses: " .. response:text()
    end
end

gurt.select('#logout'):on('click', function()
    gurt.crumbs.delete('gurtpay_session')
    gurt.location.goto('/login')
end)

-- Function for business button actions
function perform_business_action(business_id, action)
    local prompt_text = action == "deposit" and "Enter amount to deposit to business:" or "Enter amount to withdraw from business:"
    local amount = prompt(prompt_text)
    
    if amount and tonumber(amount) and tonumber(amount) > 0 then
        local response = fetch('/api/business/transfer', {
            method = 'POST',
            headers = {
                ['Authorization'] = 'Bearer ' .. session_token,
                ['Content-Type'] = 'application/json'
            },
            body = JSON.stringify({
                business_id = business_id,
                amount = tonumber(amount),
                direction = action,
                description = (action == "deposit" and "Deposit" or "Withdraw") .. " from dashboard"
            })
        })
        
        if response:ok() then
            alert((action == "deposit" and "Deposit" or "Withdrawal") .. " successful!")
            fetch_businesses()
            fetch_wallet()
        else
            local ok_parse, error_data = pcall(function() return response:json() end)
            local error_msg = "Unknown error"
            if ok_parse and error_data and error_data.error then
                error_msg = error_data.error
            end
            alert((action == "deposit" and "Deposit" or "Withdrawal") .. " failed: " .. error_msg)
        end
    end
end

fetch_wallet()
fetch_transactions()
fetch_businesses()