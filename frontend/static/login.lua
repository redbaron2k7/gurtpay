-- Check if user is already logged in
local stored_session = gurt.crumbs.get("gurtpay_session")
if stored_session then 
    setTimeout(function()
        gurt.location.goto("/")
    end, 50)
    return
end

local function set_status(msg, is_error)
    local status_el = gurt.select('#status-message')
    if status_el then 
        status_el.text = msg
        if is_error then
            status_el:setAttribute('style', 'error-text mb-4')
        else
            status_el:setAttribute('style', 'info-text mb-4')
        end
    end
end

local function ui(fn)
    setTimeout(function()
        pcall(fn)
    end, 0)
end

-- Login button handler
local login_btn = gurt.select('#login-btn')
if login_btn then
    login_btn:on('click', function()
        local username = gurt.select('#username').value
        local password = gurt.select('#password').value
        
        if not username or username:trim() == '' then
            set_status('Please enter your username', true)
            return
        end
        
        if not password or password == '' then
            set_status('Please enter your password', true)
            return
        end
        
        set_status('Signing in...')
        
        local success, response = pcall(function()
            return fetch('/api/auth/login', {
                method = 'POST',
                headers = { ['Content-Type'] = 'application/json' },
                body = JSON.stringify({ 
                    username = username:trim(), 
                    password = password 
                })
            })
        end)
        
        if not success then
            set_status('Network error. Please try again.', true)
            return
        end
        
        if response:ok() then
            local ok_parse, data = pcall(function()
                return response:json()
            end)
            
            if ok_parse and data then
                local ok_set = pcall(function()
                    gurt.crumbs.set({ 
                        name = 'gurtpay_session', 
                        value = JSON.stringify(data), 
                        lifetime = 86400 
                    })
                end)
                
                if ok_set then
                    set_status('Login successful! Redirecting...')
                    ui(function()
                        gurt.location.goto('/')
                    end)
                else
                    set_status('Session error. Please try again.', true)
                end
            else
                set_status('Invalid response from server', true)
            end
        else
            local error_msg = 'Login failed'
            local ok_parse, error_data = pcall(function()
                return response:json()
            end)
            
            if ok_parse and error_data and error_data.error then
                error_msg = error_data.error
            elseif response.status == 401 then
                error_msg = 'Invalid username or password'
            elseif response.status >= 500 then
                error_msg = 'Server error. Please try again later.'
            end
            
            set_status(error_msg, true)
        end
    end)
end
