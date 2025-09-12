local token = gurt.crumbs.get("gurtpay_session")
if not token then 
    gurt.location.goto("/login")
end

local session = JSON.parse(token)
local session_token = session.session_token

local campaigns = {}
local current_campaign_id = nil

-- Initialize
function init()
    gurt.select('#username').text = session.user and session.user.username or "User"
    
    -- Bind events
    gurt.select('#create-campaign-btn'):on('click', create_campaign)
    gurt.select('#create-creative-btn'):on('click', create_creative)
    gurt.select('#cancel-creative-btn'):on('click', cancel_creative)
    gurt.select('#fund-campaign-btn'):on('click', fund_campaign)
    gurt.select('#cancel-fund-btn'):on('click', cancel_fund)
    
    -- Bind field change events
    gurt.select('#campaign-bid-model'):on('change', toggle_bid_fields)
    gurt.select('#creative-type'):on('change', toggle_creative_fields)
    
    fetch_business_balance()
    fetch_campaigns()
end

-- Fetch business balance
function fetch_business_balance()
    local response = fetch('/api/wallet/balance', {
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token
        }
    })

    if response:ok() then
        local data = response:json()
        gurt.select('#business-balance').text = tostring(data.balance or 0) .. " GC"
    else
        gurt.select('#business-balance').text = "Error loading balance"
    end
end

-- Toggle bid model fields
function toggle_bid_fields()
    local bid_model = gurt.select('#campaign-bid-model').value
    
    if bid_model == 'cpm' then
        gurt.select('#cpm-fields').classList:remove('hidden')
        gurt.select('#cpc-fields').classList:add('hidden')
    elseif bid_model == 'cpc' then
        gurt.select('#cpm-fields').classList:add('hidden')
        gurt.select('#cpc-fields').classList:remove('hidden')
    else
        gurt.select('#cpm-fields').classList:add('hidden')
        gurt.select('#cpc-fields').classList:add('hidden')
    end
end

-- Toggle creative type fields
function toggle_creative_fields()
    local creative_type = gurt.select('#creative-type').value
    
    if creative_type == 'html' then
        gurt.select('#html-creative').classList:remove('hidden')
        gurt.select('#image-creative').classList:add('hidden')
    elseif creative_type == 'image' then
        gurt.select('#html-creative').classList:add('hidden')
        gurt.select('#image-creative').classList:remove('hidden')
    else
        gurt.select('#html-creative').classList:add('hidden')
        gurt.select('#image-creative').classList:add('hidden')
    end
end

-- Create new campaign
function create_campaign()
    local budget = tonumber(gurt.select('#campaign-budget').value)
    local bid_model = gurt.select('#campaign-bid-model').value
    local max_cpm = tonumber(gurt.select('#campaign-max-cpm').value)
    local max_cpc = tonumber(gurt.select('#campaign-max-cpc').value)

    if not budget or budget < 10 then
        show_campaign_status("Budget must be at least 10 GC", true)
        return
    end
    
    if not bid_model or bid_model == '' then
        show_campaign_status("Please select a bid model", true)
        return
    end

    show_campaign_status("Creating campaign...", false)
    
    local body_data = {
        budget_total = budget,
        bid_model = bid_model
    }
    
    if bid_model == 'cpm' and max_cpm then
        body_data.max_cpm = max_cpm
    elseif bid_model == 'cpc' and max_cpc then
        body_data.max_cpc = max_cpc
    end
    
    local response = fetch('/api/ads/campaign/create', {
        method = 'POST',
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token,
            ['Content-Type'] = 'application/json'
        },
        body = JSON.stringify(body_data)
    })

    if response:ok() then
        local campaign = response:json()
        show_campaign_status("Campaign created successfully!", false)
        clear_campaign_form()
        fetch_campaigns()
    else
        show_campaign_status("Failed to create campaign: " .. response:text(), true)
    end
end

-- Fetch user's campaigns
function fetch_campaigns()
    -- For now, show empty state since we don't have a list endpoint
    -- In production, this would call GET /api/ads/campaigns
    render_campaigns({})
end

-- Render campaigns list
function render_campaigns(campaign_list)
    campaigns = campaign_list
    local container = gurt.select('#campaigns-list')
    container:clear()
    
    if #campaigns == 0 then
        local empty = gurt.create('p', {
            text = "No campaigns created yet. Create your first campaign above!",
            style = "text-slate-500 text-center"
        })
        container:append(empty)
        return
    end
    
    for i, campaign in ipairs(campaigns) do
        local campaign_card = create_campaign_card(campaign)
        container:append(campaign_card)
    end
end

-- Create campaign card UI
function create_campaign_card(campaign)
    local card = gurt.create('div', {
        style = "bg-[#f9fafb] border border-slate-200 rounded p-4"
    })
    
    local header = gurt.create('div', {
        style = "flex justify-between items-start mb-3"
    })
    
    local info = gurt.create('div')
    local title = gurt.create('h3', {
        text = "Campaign " .. campaign.id:sub(1, 8),
        style = "font-bold text-lg text-slate-900"
    })
    local budget_info = gurt.create('p', {
        text = "Budget: " .. tostring(campaign.budget_remaining or 0) .. " / " .. tostring(campaign.budget_total or 0) .. " GC",
        style = "text-sm text-slate-600"
    })
    local status = gurt.create('p', {
        text = campaign.status == 'active' and "🟢 Active" or "🔴 Inactive",
        style = "text-sm"
    })
    
    info:append(title)
    info:append(budget_info)
    info:append(status)
    
    local actions = gurt.create('div', {
        style = "flex gap-2"
    })
    
    local add_creative_btn = gurt.create('button', {
        text = "Add Creative",
        style = "px-3 py-1 bg-blue-600 text-white text-sm rounded hover:bg-blue-700"
    })
    add_creative_btn:on('click', function()
        show_creative_form(campaign.id)
    end)
    
    local fund_btn = gurt.create('button', {
        text = "Fund",
        style = "px-3 py-1 bg-green-600 text-white text-sm rounded hover:bg-green-700"
    })
    fund_btn:on('click', function()
        show_fund_form(campaign.id)
    end)
    
    actions:append(add_creative_btn)
    actions:append(fund_btn)
    header:append(info)
    header:append(actions)
    card:append(header)
    
    -- Show creatives if any
    if campaign.creatives and #campaign.creatives > 0 then
        local creatives_title = gurt.create('p', {
            text = "Creatives:",
            style = "text-sm font-medium text-slate-700 mb-2"
        })
        card:append(creatives_title)
        
        for _, creative in ipairs(campaign.creatives) do
            local creative_info = gurt.create('div', {
                style = "bg-white border border-slate-100 rounded p-2 mb-2"
            })
            local creative_text = creative.format
            if creative.width and creative.height then
                creative_text = creative_text .. " - " .. creative.width .. "x" .. creative.height .. "px"
            end
            creative_info.text = creative_text
            card:append(creative_info)
        end
    end
    
    return card
end

-- Show creative form
function show_creative_form(campaign_id)
    current_campaign_id = campaign_id
    gurt.select('#creative-campaign-id').value = campaign_id
    gurt.select('#creative-form').classList:remove('hidden')
    gurt.select('#creative-form'):scrollIntoView()
end

-- Cancel creative form
function cancel_creative()
    gurt.select('#creative-form').classList:add('hidden')
    clear_creative_form()
end

-- Create new creative
function create_creative()
    local campaign_id = gurt.select('#creative-campaign-id').value
    local format = gurt.select('#creative-format').value
    local width = tonumber(gurt.select('#creative-width').value)
    local height = tonumber(gurt.select('#creative-height').value)
    local creative_type = gurt.select('#creative-type').value
    local html = gurt.select('#creative-html').value
    local image_url = gurt.select('#creative-image-url').value
    local click_url = gurt.select('#creative-click-url').value

    if not format or format == '' or not creative_type or creative_type == '' or not click_url or click_url == '' then
        show_creative_status("Please fill in required fields", true)
        return
    end

    if creative_type == 'html' and (not html or html == '') then
        show_creative_status("Please enter HTML content", true)
        return
    end
    
    if creative_type == 'image' and (not image_url or image_url == '') then
        show_creative_status("Please enter image URL", true)
        return
    end

    show_creative_status("Creating creative...", false)
    
    local body_data = {
        campaign_id = campaign_id,
        format = format,
        width = width,
        height = height,
        click_url = click_url
    }
    
    if creative_type == 'html' then
        body_data.html = html
    else
        body_data.image_url = image_url
    end
    
    local response = fetch('/api/ads/creative/create', {
        method = 'POST',
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token,
            ['Content-Type'] = 'application/json'
        },
        body = JSON.stringify(body_data)
    })

    if response:ok() then
        show_creative_status("Creative created successfully!", false)
        clear_creative_form()
        gurt.select('#creative-form').classList:add('hidden')
        fetch_campaigns()
    else
        show_creative_status("Failed to create creative: " .. response:text(), true)
    end
end

-- Show fund form
function show_fund_form(campaign_id)
    current_campaign_id = campaign_id
    gurt.select('#fund-campaign-id').value = campaign_id
    gurt.select('#fund-form').classList:remove('hidden')
    gurt.select('#fund-form'):scrollIntoView()
end

-- Cancel fund form
function cancel_fund()
    gurt.select('#fund-form').classList:add('hidden')
    clear_fund_form()
end

-- Fund campaign
function fund_campaign()
    local campaign_id = gurt.select('#fund-campaign-id').value
    local amount = tonumber(gurt.select('#fund-amount').value)

    if not amount or amount < 5 then
        show_fund_status("Amount must be at least 5 GC", true)
        return
    end

    show_fund_status("Funding campaign...", false)
    
    local response = fetch('/api/ads/campaign/fund', {
        method = 'POST',
        headers = {
            ['Authorization'] = 'Bearer ' .. session_token,
            ['Content-Type'] = 'application/json'
        },
        body = JSON.stringify({
            campaign_id = campaign_id,
            amount = amount
        })
    })

    if response:ok() then
        show_fund_status("Campaign funded successfully!", false)
        clear_fund_form()
        gurt.select('#fund-form').classList:add('hidden')
        fetch_campaigns()
        fetch_business_balance()
    else
        show_fund_status("Failed to fund campaign: " .. response:text(), true)
    end
end

-- Clear forms
function clear_campaign_form()
    gurt.select('#campaign-budget').value = ""
    gurt.select('#campaign-bid-model').value = ""
    gurt.select('#campaign-max-cpm').value = ""
    gurt.select('#campaign-max-cpc').value = ""
    toggle_bid_fields()
    show_campaign_status("", false)
end

function clear_creative_form()
    gurt.select('#creative-format').value = ""
    gurt.select('#creative-width').value = ""
    gurt.select('#creative-height').value = ""
    gurt.select('#creative-type').value = ""
    gurt.select('#creative-html').value = ""
    gurt.select('#creative-image-url').value = ""
    gurt.select('#creative-click-url').value = ""
    toggle_creative_fields()
    show_creative_status("", false)
end

function clear_fund_form()
    gurt.select('#fund-amount').value = ""
    show_fund_status("", false)
end

-- Status message helpers
function show_campaign_status(message, is_error)
    local status_el = gurt.select('#campaign-status')
    status_el.text = message
    if is_error then
        status_el.style = "text-center text-sm mb-4 text-red-600"
    else
        status_el.style = "text-center text-sm mb-4 text-green-600"
    end
end

function show_creative_status(message, is_error)
    local status_el = gurt.select('#creative-status')
    status_el.text = message
    if is_error then
        status_el.style = "text-center text-sm mb-4 text-red-600"
    else
        status_el.style = "text-center text-sm mb-4 text-green-600"
    end
end

local function show_fund_status(message, is_error)
    local status_el = gurt.select('#fund-status')
    status_el.text = message
    if is_error then
        status_el.style = "text-center text-sm mb-4 text-red-600"
    else
        status_el.style = "text-center text-sm mb-4 text-green-600"
    end
end

-- Initialize when page loads
init()
