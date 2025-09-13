-- Check for session token and redirect if not logged in
local token = gurt.crumbs.get("gurtpay_session")
if not token or token == "" then
    setTimeout(function()
        gurt.location.goto("/login")
    end, 50)
    return
end

local ok_parse, session = pcall(function() return JSON.parse(token) end)
if not ok_parse or not session then
    setTimeout(function()
        gurt.location.goto("/login")
    end, 50)
    return
end

local session_token = session.session_token

-- UI helper function for safe DOM updates
local function ui(fn)
    setTimeout(function()
        pcall(fn)
    end, 0)
end

-- Authentication error handler
local function handle_auth_error(response)
    if response.status >= 500 then
        gurt.crumbs.delete('gurtpay_session')
        setTimeout(function()
            gurt.location.goto('/login')
        end, 100)
        return true
    end
    return false
end

-- Set username in header
ui(function()
    local name_el = gurt.select('#username')
    if name_el then
        name_el.text = session.user and session.user.username or "User"
    end
end)

-- Logout handler
ui(function()
    local logout_btn = gurt.select('#logout')
    if logout_btn then
        logout_btn:on('click', function()
            gurt.crumbs.delete('gurtpay_session')
            setTimeout(function()
                gurt.location.goto('/login')
            end, 100)
        end)
    end
    local back_btn = gurt.select('#back-btn')
    if back_btn then
        back_btn:on('click', function()
            gurt.location.goto('/')
        end)
    end
end)

-- Cards data
local debit_cards_list = {}

-- Generate card color based on card number
local function get_card_color(card_number)
    -- Generate consistent colors based on card number hash
    local hash = 0
    for i = 1, #card_number do
        hash = (hash + string.byte(card_number, i)) % 1000
    end
    
    -- Select from predefined solid colors that will definitely work
    local colors = {
        'bg-blue-700',
        'bg-purple-700', 
        'bg-emerald-700',
        'bg-rose-700',
        'bg-amber-700',
        'bg-cyan-700',
        'bg-indigo-700',
        'bg-teal-700',
        'bg-pink-700',
        'bg-orange-700',
        'bg-lime-700',
        'bg-sky-700'
    }
    
    return colors[(hash % #colors) + 1]
end

local function format_card_number(masked_number)
    return masked_number
end

local function render_debit_cards()
    local empty_state = gurt.select('#empty-state')
    local cards_grid = gurt.select('#cards-grid')
    local loading_state = gurt.select('#loading-state')
    local create_btn = gurt.select('#create-card-btn')
    
    if not cards_grid then return end
    
    -- Always hide loading state when rendering
    if loading_state then
        loading_state.classList:add('hidden')
    end
    
    -- Clear previous children
    local children = cards_grid.children
    for i = #children, 1, -1 do
        children[i]:remove()
    end
    
    if #debit_cards_list == 0 then
        -- Show empty state only after loading is complete, hide cards grid
        if empty_state then
            empty_state.classList:remove('hidden')
        end
        if cards_grid then
            cards_grid.classList:add('hidden')
        end
        -- Show create button for empty state
        if create_btn then
            create_btn.text = "+ Create New Card"
            create_btn.style = "bg-[#0b5cab] text-white px-5 py-3 rounded-md font-bold hover:bg-[#094b97] cursor-pointer border border-[#0b5cab]"
        end
        return
    end
    
    -- Hide empty state, show cards grid with cards
    if empty_state then
        empty_state.classList:add('hidden')
    end
    if cards_grid then
        cards_grid.classList:remove('hidden')
    end
    -- Change button to regenerate since user has a card
    if create_btn then
        regenerate_confirm_pending = false  -- Reset confirmation state
        create_btn.text = "🔄 Regenerate Card"
        create_btn.style = "bg-orange-500 text-white px-5 py-3 rounded-md font-bold hover:bg-orange-600 cursor-pointer border border-orange-500"
    end
     
    -- Create cards using gurt.create
    for i = 1, #debit_cards_list do
        local card_data = debit_cards_list[i]
        local card_color = get_card_color(card_data.card_number)
        local formatted_number = format_card_number(card_data.card_number)
        local exp_date = string.format("%02d/%d", card_data.expiration_month, card_data.expiration_year)
        
        -- Create card container with solid color background
        local card_container = gurt.create('div', { 
            style = card_color .. ' rounded-2xl border border-slate-600 p-6 shadow-xl w-[380px] min-w-[380px] min-h-[240px] text-white flex flex-col'
        })
        
        -- Card header with chip and brand
        local header = gurt.create('div', { style = 'flex items-center justify-between mb-4' })
        
        -- Chip icon
        local chip = gurt.create('div', { style = 'w-12 h-10 bg-yellow-500 rounded-md relative mb-4 shadow-sm' })
        
        -- Brand and type
        local brand_section = gurt.create('div', { style = 'text-right' })
        local brand = gurt.create('p', { text = 'GURT', style = 'text-white text-xl font-bold tracking-wider' })
        local badge = gurt.create('span', { text = 'VIRTUAL', style = 'text-xs text-white bg-black bg-opacity-30 px-2 py-0.5 rounded' })
        brand_section:append(brand)
        brand_section:append(badge)
        
        header:append(chip)
        header:append(brand_section)
        card_container:append(header)
        
        -- Card number with proper spacing
        local number = gurt.create('div', {
            text = formatted_number,
            style = 'font-mono text-xl font-bold text-white mb-4 tracking-wider'
        })
        card_container:append(number)
        
        -- Card details section
        local details = gurt.create('div', {
            style = 'flex justify-between items-end mt-auto mb-4 gap-8'
        })
        
        -- Left side - name and CVV
        local left_side = gurt.create('div')
        local name = gurt.create('div', {
            text = session.user and session.user.username or "CARDHOLDER",
            style = 'text-sm font-bold text-white uppercase tracking-wide'
        })
        local cvv = gurt.create('div', { text = 'CVV: ' .. card_data.cvv, style = 'text-xs text-slate-300 uppercase tracking-wide' })
        left_side:append(name)
        left_side:append(cvv)
        
        -- Right side - expiry
        local right_side = gurt.create('div', { style = 'text-right' })
        local expires_label = gurt.create('div', { text = 'EXPIRES', style = 'text-xs text-slate-300 uppercase tracking-wide' })
        local expires_date = gurt.create('div', { text = exp_date, style = 'text-sm text-white font-medium' })
        right_side:append(expires_label)
        right_side:append(expires_date)
        
        details:append(left_side)
        details:append(right_side)
        card_container:append(details)
        
        -- Regenerate button at bottom
        local button_container = gurt.create('div', { style = 'mt-4' })
        local regenerate_btn = gurt.create('button', { 
            text = 'Regenerate Numbers', 
            style = 'bg-orange-500 hover:bg-orange-600 text-white text-xs px-3 py-1.5 rounded-md'
        })
        
        -- Add click handler
        regenerate_btn:on('click', function()
            regenerate_card()
        end)
        
        button_container:append(regenerate_btn)
        card_container:append(button_container)
        
        -- Add card to grid
        cards_grid:append(card_container)
    end
end

-- Fetch debit cards from API
local function fetch_debit_cards()
    local response = fetch('/api/cards/list', {
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        local data = response:json()
        debit_cards_list = data.cards or {}
        render_debit_cards()
    else
        if handle_auth_error(response) then
            return
        end
        ui(function()
            local loading_state = gurt.select('#loading-state')
            if loading_state then
                -- Clear loading state children
                local children = loading_state.children
                for i = #children, 1, -1 do
                    children[i]:remove()
                end
                
                -- Create error state
                local error_container = gurt.create('div', {
                    style = 'text-center py-16'
                })
                
                local error_icon = gurt.create('div', {
                    style = 'bg-red-100 rounded-full w-16 h-16 flex items-center justify-center mx-auto mb-4'
                })
                local error_symbol = gurt.create('span', {
                    text = 'ERROR',
                    style = 'text-sm text-red-600 font-bold'
                })
                error_icon:append(error_symbol)
                
                local error_title = gurt.create('p', {
                    text = 'Failed to load cards',
                    style = 'text-lg text-red-600'
                })
                
                local error_detail = gurt.create('p', {
                    text = response:text(),
                    style = 'text-sm text-slate-600'
                })
                
                error_container:append(error_icon)
                error_container:append(error_title)
                error_container:append(error_detail)
                loading_state:append(error_container)
            end
        end)
    end
end

-- Create new debit card or regenerate existing
local function create_or_regenerate_card()
    local has_cards = #debit_cards_list > 0
    local endpoint = has_cards and '/api/cards/regenerate' or '/api/cards/create'
    local success_msg = has_cards and 'Card numbers regenerated successfully!' or 'New debit card created successfully!'
    
    if has_cards and not regenerate_confirm_pending then
        -- First click - change button to confirm
        regenerate_confirm_pending = true
        local create_btn = gurt.select('#create-card-btn')
        if create_btn then
            create_btn.text = "⚠️ Click Again to Confirm"
            create_btn.style = "bg-red-500 text-white px-5 py-3 rounded-md font-bold hover:bg-red-600 cursor-pointer border border-red-500"
        end
        return
    end
    
    -- Reset confirmation state
    regenerate_confirm_pending = false
    
    local response = fetch(endpoint, {
        method = 'POST',
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        ui(function()
            -- Show success message
            local temp_message = gurt.create('div', {
                style = 'position-fixed top-4 right-4 bg-emerald-500 text-white px-6 py-3 rounded-lg shadow-lg z-50',
                text = success_msg
            })
            gurt.body:append(temp_message)
            
            -- Remove message after 3 seconds
            setTimeout(function()
                if temp_message then
                    temp_message:remove()
                end
            end, 3000)
        end)
        
        -- Refresh cards list
        fetch_debit_cards()
    else
        if handle_auth_error(response) then
            return
        end
        local ok_parse, error_data = pcall(function() return response:json() end)
        local error_msg = has_cards and "Failed to regenerate card" or "Failed to create card"
        if ok_parse and error_data and error_data.error then
            error_msg = error_data.error
        end
        
        ui(function()
            local temp_message = gurt.create('div', {
                style = 'position-fixed top-4 right-4 bg-red-500 text-white px-6 py-3 rounded-lg shadow-lg z-50',
                text = 'Error: ' .. error_msg
            })
            gurt.body:append(temp_message)
            
            setTimeout(function()
                if temp_message then
                    temp_message:remove()
                end
            end, 3000)
        end)
    end
end

-- Regenerate card numbers (for button on card)
function regenerate_card()    
    local response = fetch('/api/cards/regenerate', {
        method = 'POST',
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        ui(function()
            -- Show success message
            local temp_message = gurt.create('div', {
                style = 'position-fixed top-4 right-4 bg-emerald-500 text-white px-6 py-3 rounded-lg shadow-lg z-50',
                text = 'Card numbers regenerated successfully!'
            })
            gurt.body:append(temp_message)
            
            -- Remove message after 3 seconds
            setTimeout(function()
                if temp_message then
                    temp_message:remove()
                end
            end, 3000)
        end)
        
        -- Refresh cards list
        fetch_debit_cards()
    else
        if handle_auth_error(response) then
            return
        end
        local ok_parse, error_data = pcall(function() return response:json() end)
        local error_msg = "Failed to regenerate card"
        if ok_parse and error_data and error_data.error then
            error_msg = error_data.error
        end
        
        ui(function()
            local temp_message = gurt.create('div', {
                style = 'position-fixed top-4 right-4 bg-red-500 text-white px-6 py-3 rounded-lg shadow-lg z-50',
                text = 'Error: ' .. error_msg
            })
            gurt.body:append(temp_message)
            
            setTimeout(function()
                if temp_message then
                    temp_message:remove()
                end
            end, 3000)
        end)
    end
end


-- Add event listeners
ui(function()
    -- Main create/regenerate card button
    local create_btn = gurt.select('#create-card-btn')
    if create_btn then
        create_btn:on('click', function()
            create_or_regenerate_card()
        end)
    end
    
    -- Empty state create button
    local empty_create_btn = gurt.select('#empty-create-btn')
    if empty_create_btn then
        empty_create_btn:on('click', function()
            create_or_regenerate_card()
        end)
    end
end)

-- Initialize page
setTimeout(function()
    fetch_debit_cards()
end, 0)
