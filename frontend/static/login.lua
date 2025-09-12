-- Prevent script from running multiple times
if gurt.crumbs.get("login_script_running") then
    return
end
gurt.crumbs.set({
    name = "login_script_running",
    value = "true",
    lifetime = 5
})

local stored_session = gurt.crumbs.get("gurtpay_session")
if stored_session then
    gurt.location.goto("/")
end

local loading_section = gurt.select("#loading-section")
local verifying_section = gurt.select("#verifying-section")
local login_section = gurt.select("#login-section")

local token = gurt.location.query.get("token")

if token then
    print("Token received:", token)
    login_section.classList:add("hidden")
    verifying_section.classList:remove("hidden")
    
    local response = fetch("/api/auth/verify", {
        method = "POST",
        headers = {
            ["Content-Type"] = "application/json"
        },
        body = JSON.stringify({
            token = token
        })
    })
    
    print("Auth response status:", response.status)
    
    if response:ok() then
        local auth_data = response:json()
        
        gurt.crumbs.set({
            name = "gurtpay_session",
            value = JSON.stringify(auth_data),
            lifetime = 86400
        })
        
        setTimeout(function()
            gurt.location.goto("/")
        end, 1000)
    else
        verifying_section.classList:add("hidden")
        login_section.classList:remove("hidden")
        
        local error_div = gurt.create("div", {
            style = "bg-red-100 border border-red-300 text-red-700 px-4 py-3 rounded mt-4 text-center",
            text = "Authentication failed. Please try again."
        })
        gurt.select("body div"):append(error_div)
        
        setTimeout(function()
            error_div:remove()
        end, 5000)
    end
end

gurt.select("#login-btn"):on("click", function()
    print("Login button clicked!")
    login_section.classList:add("hidden")
    loading_section.classList:remove("hidden")
    
    setTimeout(function()
        gurt.location.goto("gurt://arsonflare.aura/oauth2?appid=10&redirect_uri=gurt://gurtpay.dev/login")
    end, 1500)
end)
