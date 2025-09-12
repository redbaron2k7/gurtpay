local token = gurt.crumbs.get("gurtpay_session")
if not token then 
    gurt.location.goto("/login")
end

local session = JSON.parse(token)
local session_token = session.session_token

local sites = {}
local current_site_id = nil

-- Initialize
function init()
    gurt.select('#username').text = session.user and session.user.username or "User"
    
    -- Bind events
    gurt.select('#register-site-btn'):on('click', register_site)
    gurt.select('#add-slot-btn'):on('click', add_slot)
    gurt.select('#cancel-slot-btn'):on('click', cancel_slot)
    
    fetch_sites()
end

-- Register new site
function register_site()
    local domain = gurt.select('#site-domain').value
    if not domain or domain == '' then
        show_site_status("Please enter a domain", true)
        return
    end

    show_site_status("Registering site...", false)
    
    local response = fetch('/api/ads/site/register', {
        method = 'POST',
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token,
            ['Content-Type'] = 'application/json'
        },
        body = JSON.stringify({
            domain = domain
        })
    })

    if response:ok() then
        local site = response:json()
        show_site_status("Site registered successfully!", false)
        gurt.select('#site-domain').value = ""
        fetch_sites()
    else
        show_site_status("Failed to register site: " .. response:text(), true)
    end
end

-- Fetch user's sites
function fetch_sites()
    -- For now, show empty state since we don't have a list endpoint
    -- In production, this would call GET /api/ads/sites
    render_sites({})
end

-- Render sites list
function render_sites(site_list)
    sites = site_list
    local container = gurt.select('#sites-list')
    
    if #sites == 0 then
        local empty = gurt.create('p', {
            text = "No sites registered yet. Register your first site above!",
            style = "text-slate-500 text-center"
        })
        container:append(empty)
        return
    end
    
    for i, site in ipairs(sites) do
        local site_card = create_site_card(site)
        container:append(site_card)
    end
end

-- Create site card UI
function create_site_card(site)
    local card = gurt.create('div', {
        style = "bg-[#f9fafb] border border-slate-200 rounded p-4"
    })
    
    local header = gurt.create('div', {
        style = "flex justify-between items-start mb-3"
    })
    
    local info = gurt.create('div')
    local domain = gurt.create('h3', {
        text = site.domain,
        style = "font-bold text-lg text-slate-900"
    })
    local status = gurt.create('p', {
        text = site.verified and "✅ Verified" or "⏳ Pending verification",
        style = "text-sm " .. (site.verified and "text-green-600" or "text-yellow-600")
    })
    info:append(domain)
    info:append(status)
    
    local actions = gurt.create('div', {
        style = "flex gap-2"
    })
    
    local add_slot_btn = gurt.create('button', {
        text = "Add Slot",
        style = "px-3 py-1 bg-blue-600 text-white text-sm rounded hover:bg-blue-700"
    })
    add_slot_btn:on('click', function()
        show_slot_form(site.id)
    end)
    
    actions:append(add_slot_btn)
    header:append(info)
    header:append(actions)
    card:append(header)
    
    -- Show slots if any
    if site.slots and #site.slots > 0 then
        local slots_title = gurt.create('p', {
            text = "Ad Slots:",
            style = "text-sm font-medium text-slate-700 mb-2"
        })
        card:append(slots_title)
        
        for _, slot in ipairs(site.slots) do
            local slot_info = gurt.create('div', {
                style = "bg-white border border-slate-100 rounded p-2 mb-2"
            })
            local slot_text = slot.slot_key .. " (" .. slot.format .. ")"
            if slot.width and slot.height then
                slot_text = slot_text .. " - " .. slot.width .. "x" .. slot.height .. "px"
            end
            slot_info.text = slot_text
            card:append(slot_info)
        end
    end
    
    return card
end

-- Show slot form
function show_slot_form(site_id)
    current_site_id = site_id
    gurt.select('#slot-site-id').value = site_id
    gurt.select('#slot-form').classList:remove('hidden')
    gurt.select('#slot-form'):scrollIntoView()
end

-- Cancel slot form
function cancel_slot()
    gurt.select('#slot-form').classList:add('hidden')
    clear_slot_form()
end

-- Add new slot
function add_slot()
    local site_id = gurt.select('#slot-site-id').value
    local slot_key = gurt.select('#slot-key').value
    local format = gurt.select('#slot-format').value
    local width = tonumber(gurt.select('#slot-width').value)
    local height = tonumber(gurt.select('#slot-height').value)
    local floor_price = tonumber(gurt.select('#slot-floor-price').value)

    if not slot_key or slot_key == '' or not format or format == '' then
        show_slot_status("Please fill in required fields", true)
        return
    end

    show_slot_status("Adding slot...", false)
    
    local response = fetch('/api/ads/slot/register', {
        method = 'POST',
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token,
            ['Content-Type'] = 'application/json'
        },
        body = JSON.stringify({
            site_id = site_id,
            slot_key = slot_key,
            format = format,
            width = width,
            height = height,
            floor_price = floor_price
        })
    })

    if response:ok() then
        show_slot_status("Slot added successfully!", false)
        clear_slot_form()
        gurt.select('#slot-form').classList:add('hidden')
        fetch_sites()
    else
        show_slot_status("Failed to add slot: " .. response:text(), true)
    end
end

-- Clear slot form
function clear_slot_form()
    gurt.select('#slot-key').value = ""
    gurt.select('#slot-format').value = ""
    gurt.select('#slot-width').value = ""
    gurt.select('#slot-height').value = ""
    gurt.select('#slot-floor-price').value = ""
    show_slot_status("", false)
end

-- Show site status message
function show_site_status(message, is_error)
    local status_el = gurt.select('#site-status')
    status_el.text = message
    if is_error then
        status_el.style = "text-center text-sm mb-4 text-red-600"
    else
        status_el.style = "text-center text-sm mb-4 text-green-600"
    end
end

-- Show slot status message
function show_slot_status(message, is_error)
    local status_el = gurt.select('#slot-status')
    status_el.text = message
    if is_error then
        status_el.style = "text-center text-sm mb-4 text-red-600"
    else
        status_el.style = "text-center text-sm mb-4 text-green-600"
    end
end

-- Initialize when page loads
init()
