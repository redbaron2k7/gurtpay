local stored_session = gurt.crumbs.get("gurtpay_session")
if not stored_session then
    gurt.location.goto("/login")
end

local session_data = JSON.parse(stored_session)
local current_user = session_data.user
local session_token = session_data.session_token

do
    local uname_el = gurt.select("#username")
    if uname_el then
        uname_el.text = current_user.username
    end
end

local function show_status(message, success)
    local line = gurt.select('#status-line-biz')
    if not line then return end
    line.text = message
    -- Avoid color classes to prevent renderer crashes
    line:setAttribute('style', 'text-center text-sm mb-3')
end

local function perform_register()
    local business_name = gurt.select("#business-name").value:trim()
    local website_url = gurt.select("#website-url").value:trim()

    if business_name == "" then
        show_status("Please enter a business name", false)
        return
    end

    if business_name:len() < 2 then
        show_status("Business name must be at least 2 characters", false)
        return
    end

    gurt.select("#register-btn").text = "Registering..."
    gurt.select("#register-btn").disabled = true

    local response = fetch("/api/business/register", {
        method = "POST",
        headers = {
            ["Authorization"] = "Bearer " .. session_token,
            ["Content-Type"] = "application/json"
        },
        body = JSON.stringify({
            business_name = business_name,
            website_url = website_url:len() > 0 and website_url or nil
        })
    })

    if response:ok() then
        -- On success, redirect to dashboard instead of modifying DOM
        local _ = response:json()
        gurt.location.goto("/")
    else
        local msg = "Registration failed"
        local ok_parse, parsed = pcall(function() return response:json() end)
        if ok_parse and parsed and parsed.error then
            msg = parsed.error
        else
            local body = response:text()
            if body and body ~= "" then msg = body end
        end
        show_status(msg, false)

        gurt.select("#register-btn").text = "Register Business"
        gurt.select("#register-btn").disabled = false
    end
end

gurt.select("#business-form"):on("submit", function(event)
    event:preventDefault()
    perform_register()
end)

gurt.select("#register-btn"):on("click", function()
    perform_register()
end)

do
    -- No copy button on this page after redirect flow; keep noop for safety
end
