local stored_session = gurt.crumbs.get("gurtpay_session")
if not stored_session then
    gurt.location.goto("/login")
end

local session_data = JSON.parse(stored_session)
local current_user = session_data.user
local session_token = session_data.session_token
local current_balance = 0

gurt.select("#username").text = current_user.username

local function load_balance()
    local response = fetch("/api/wallet/balance", {
        headers = {
            ["Authorization"] = "Bearer " .. session_token
        }
    })
    
    if response:ok() then
        local wallet_data = response:json()
        current_balance = wallet_data.balance
        gurt.select("#current-balance").text = current_balance .. " GC"
    else
        show_status("Failed to load balance", false)
    end
end

local function show_status(message, success)
    local status_line = gurt.select('#status-line')
    if not status_line then return end
    status_line.text = message
    status_line.classList:remove('hidden')
    status_line:setAttribute('style', (success and 'text-green-700 text-center text-sm mb-3' or 'text-red-600 text-center text-sm mb-3'))
end

-- Back to dashboard
local back_btn = gurt.select('#back-btn')
if back_btn then
    back_btn:on('click', function()
        gurt.location.goto('/')
    end)
end

-- Sending logic extracted so both submit and button click can reuse it
local function perform_send()
    local recipient = gurt.select("#recipient-address").value:trim()
    local amount = tonumber(gurt.select("#amount").value)
    local description = gurt.select("#description").value:trim()
    
    if recipient == "" then
        show_status("Please enter a recipient wallet address", false)
        return
    end
    
    if not amount or amount <= 0 then
        show_status("Please enter a valid amount", false)
        return
    end
    
    if amount > current_balance then
        show_status("Insufficient balance", false)
        return
    end
    
    if amount > 10000 then
        show_status("Amount exceeds maximum limit of 10,000 GC", false)
        return
    end
    
    if description == "" then
        description = "Payment"
    end
    
    gurt.select("#send-btn").text = "Sending..."
    gurt.select("#send-btn").disabled = true
    
    local response = fetch("/api/wallet/send", {
        method = "POST",
        headers = {
            ["Authorization"] = "Bearer " .. session_token,
            ["Content-Type"] = "application/json"
        },
        body = JSON.stringify({
            to_address = recipient,
            amount = amount,
            description = description
        })
    })
    
    if response:ok() then
        show_status(string.format("Successfully sent %.2f GC!", amount), true)
        gurt.select("#recipient-address").value = ""
        gurt.select("#amount").value = ""
        gurt.select("#description").value = ""
        load_balance()
        setTimeout(function()
            gurt.location.goto("/")
        end, 3000)
    else
        local msg = "Transfer failed"
        local ok_parse, parsed = pcall(function() return response:json() end)
        if ok_parse and parsed and parsed.error then
            msg = parsed.error
        else
            local body = response:text()
            if body and body ~= "" then msg = body end
        end
        show_status(msg, false)
    end
    
    gurt.select("#send-btn").text = "Send Money"
    gurt.select("#send-btn").disabled = false
end

gurt.select("#send-form"):on("submit", function(event)
    event:preventDefault()
    perform_send()
end)

local send_btn = gurt.select('#send-btn')
if send_btn then
    send_btn:on('click', function()
        perform_send()
    end)
end

load_balance()
