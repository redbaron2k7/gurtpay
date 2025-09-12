local token = gurt.crumbs.get("gurtpay_session")
if not token then 
    gurt.location.goto("/login")
end

local session = JSON.parse(token)
local session_token = session.session_token

-- Initialize
function init()
    gurt.select('#username').text = session.user and session.user.username or "User"
    fetch_business_info()
    fetch_stats()
    fetch_recent_activity()
end

-- Fetch business information
function fetch_business_info()
    local response = fetch('/api/wallet/balance', {
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        local data = response:json()
        local business_info = "Business Balance: " .. tostring(data.balance or 0) .. " GC\n"
        business_info = business_info .. "Address: " .. (data.address or "N/A")
        gurt.select('#business-info').text = business_info
    else
        gurt.select('#business-info').text = "Failed to load business info"
    end
end

-- Fetch stats overview
function fetch_stats()
    -- These would be real API calls in production
    gurt.select('#sites-count').text = "0"
    gurt.select('#campaigns-count').text = "0" 
    gurt.select('#earnings-total').text = "0"
end

-- Fetch recent activity
function fetch_recent_activity()
    gurt.select('#recent-activity').text = "No recent activity"
end

-- Initialize when page loads
init()
