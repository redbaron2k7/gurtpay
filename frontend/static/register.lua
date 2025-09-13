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

local function validate_username(username)
    if not username or username:trim() == '' then
        return false, 'Please enter a username'
    end
    
    local trimmed = username:trim()
    if #trimmed < 2 then
        return false, 'Username must be at least 2 characters'
    end
    
    if #trimmed > 32 then
        return false, 'Username must be 32 characters or less'
    end
    
    return true, trimmed
end

local function validate_password(password)
    if not password or password == '' then
        return false, 'Please enter a password'
    end
    
    if #password < 6 then
        return false, 'Password must be at least 6 characters'
    end
    
    return true, password
end

-- Register button handler
local register_btn = gurt.select('#register-btn')
if register_btn then
    register_btn:on('click', function()
        local username = gurt.select('#username').value
        local password = gurt.select('#password').value
        
        -- Validate username
        local username_valid, username_result = validate_username(username)
        if not username_valid then
            set_status(username_result, true)
            return
        end
        
        -- Validate password
        local password_valid, password_result = validate_password(password)
        if not password_valid then
            set_status(password_result, true)
            return
        end
        
        set_status('Creating your account...')
        
        local success, response = pcall(function()
            return fetch('/api/auth/register', {
                method = 'POST',
                headers = { ['Content-Type'] = 'application/json' },
                body = JSON.stringify({ 
                    username = username_result, 
                    password = password_result 
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
                    set_status('Account created successfully! Redirecting...')
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
            local error_msg = 'Registration failed'
            local ok_parse, error_data = pcall(function()
                return response:json()
            end)
            
            if ok_parse and error_data and error_data.error then
                error_msg = error_data.error
            elseif response.status == 409 then
                error_msg = 'Username already exists. Please choose a different one.'
            elseif response.status == 400 then
                error_msg = 'Invalid username or password format'
            elseif response.status >= 500 then
                error_msg = 'Server error. Please try again later.'
            end
            
            set_status(error_msg, true)
        end
    end)
end
